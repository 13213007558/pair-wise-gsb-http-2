# frozen_string_literal: true

module HTTP2
  # Manual flow control consumption receipt.
  #
  # When manual flow control is enabled (see Connection#manual_flow_control!),
  # receiving a DATA frame does not automatically return flow control quota
  # to the peer. Instead, the :data event yields the payload together with a
  # receipt representing the frame's flow controlled bytes. Quota is only
  # returned (via WINDOW_UPDATE frames) once the application confirms
  # consumption of the data.
  #
  # Only the DATA payload is confirmable by the application: padding octets
  # are charged against the flow control windows but carry no application
  # semantic value, so they are released automatically once the payload has
  # been fully accounted for.
  #
  # Both the connection and the stream window are accounted for each
  # confirmation, so a slow stream can never hold back connection-level
  # quota that other streams have already consumed. Quota return rules for
  # the remaining (unconfirmed) bytes:
  #
  # - stream reset (either direction) or stream close: connection-level
  #   remainder is returned immediately; stream-level remainder is forfeited
  #   (the peer can no longer use it on a closed stream).
  # - remote END_STREAM: no more DATA will arrive on the stream, so
  #   subsequent confirmations return connection-level quota only.
  # - FlowControlReceipt#discard: the application abandons the remaining
  #   bytes; all remaining quota is returned immediately.
  class FlowControlReceipt
    # Number of payload bytes the application may confirm.
    attr_reader :size

    # Number of protocol-only padding bytes (not confirmable).
    attr_reader :padding

    # Number of payload bytes not yet confirmed or discarded.
    attr_reader :remaining

    def initialize(connection:, stream:, size:, padding:)
      @connection = connection
      @stream = stream
      @size = size
      @padding = padding
      @remaining = size
      @padding_released = false
      @connection_released = false
      release_padding if @remaining.zero?
    end

    # True when all payload bytes have been confirmed or discarded.
    def complete?
      @remaining.zero?
    end

    # Confirms that +amount+ payload bytes have been consumed by the
    # application, returning the corresponding quota to the peer.
    #
    # @param amount [Integer] bytes consumed; must be within the remaining
    #   unconfirmed range (duplicate or out-of-range confirmations are
    #   rejected)
    def consume(amount)
      unless amount.is_a?(Integer) && amount.positive? && amount <= @remaining
        raise ArgumentError, "invalid consumption amount #{amount.inspect} (#{@remaining} bytes remaining)"
      end

      release(amount)
      @remaining -= amount
      release_padding if @remaining.zero?
      self
    end

    # Abandons all remaining payload bytes, returning their quota to the
    # peer immediately.
    def discard
      release(@remaining)
      @remaining = 0
      release_padding
      self
    end

    # Returns the connection-level quota still held by this receipt. Used
    # when the stream closes before the application finished accounting for
    # the data: the connection window is shared with other streams and must
    # not stay depleted, while the stream-level quota is forfeited.
    def release_connection_quota
      return if @connection_released

      @connection_released = true
      amount = @remaining + unreleased_padding
      @padding_released = true
      @connection.window_update(amount) if amount.positive? && @connection.state != :closed
    end

    private

    def unreleased_padding
      @padding_released ? 0 : @padding
    end

    def release(amount)
      return if amount.zero?

      @connection.window_update(amount) unless @connection_released || @connection.state == :closed
      # Stream-level quota is only returned while the peer may still send
      # DATA on the stream. Once the stream is reset, closed, or the peer
      # has ended its side (END_STREAM), a stream WINDOW_UPDATE is useless.
      @stream.window_update(amount) if Stream::STREAM_OPEN_STATES.include?(@stream.state)
    end

    def release_padding
      return if @padding_released

      @padding_released = true
      release(@padding)
    end
  end
end
