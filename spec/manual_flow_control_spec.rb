# frozen_string_literal: true

require "helper"

RSpec.describe "manual flow control" do
  # Wires two endpoints together, exchanging frames directly (no network).
  def connect(client, server)
    client.on(:frame) { |bytes| server << bytes }
    server.on(:frame) { |bytes| client << bytes }
    client.ping("12345678") # forces connection preface + SETTINGS exchange
  end

  # Returns [client, server, streams, data_events] where the server side
  # runs in manual flow control mode.
  def manual_pair(server_settings: {})
    client = Client.new
    server = Server.new(**server_settings)
    server.manual_flow_control!

    streams = {}
    data_events = Hash.new { |hash, key| hash[key] = [] }
    server.on(:stream) do |stream|
      streams[stream.id] = stream
      stream.on(:data) { |payload, receipt| data_events[stream.id] << [payload, receipt] }
    end

    connect(client, server)
    [client, server, streams, data_events]
  end

  def open_stream(client)
    stream = client.new_stream
    stream.headers(REQUEST_HEADERS, end_stream: false)
    stream
  end

  it "pauses the sender when the window is exhausted and advances only by confirmed bytes" do
    client, _server, _streams, data_events = manual_pair(server_settings: { settings_initial_window_size: 100 })

    stream = open_stream(client)
    stream.data("x" * 100, end_stream: false)
    expect(stream.remote_window).to eq 0
    expect(client.remote_window).to eq(DEFAULT_FLOW_WINDOW - 100)

    # window exhausted: further data is buffered, not sent
    stream.data("y" * 50, end_stream: false)
    expect(stream.buffered_amount).to eq 50
    expect(data_events[stream.id].size).to eq 1

    # partial consumption advances the window by exactly the confirmed amount
    receipt = data_events[stream.id][0][1]
    receipt.consume(30)
    expect(stream.buffered_amount).to eq 20
    expect(stream.remote_window).to eq 0
    expect(data_events[stream.id].map { |payload,| payload.bytesize }).to eq [100, 30]

    # confirming the newly delivered chunk releases the remaining buffered data
    data_events[stream.id][1][1].consume(30)
    expect(stream.buffered_amount).to eq 0
    expect(data_events[stream.id].map { |payload,| payload.bytesize }).to eq [100, 30, 20]

    # consuming the rest restores both connection and stream windows
    receipt.consume(70)
    data_events[stream.id][2][1].consume(20)
    expect(stream.remote_window).to eq 100
    expect(client.remote_window).to eq DEFAULT_FLOW_WINDOW
  end

  it "accounts connection quota per stream: a slow stream cannot hold back quota consumed by others" do
    client, server, _streams, data_events = manual_pair(server_settings: { settings_initial_window_size: 100 })
    # grow the connection-level window so only stream windows throttle
    server.window_update(1000)
    conn_window = client.remote_window

    slow = open_stream(client)
    fast = open_stream(client)
    slow.data("s" * 100, end_stream: false)
    fast.data("f" * 100, end_stream: false)
    expect(client.remote_window).to eq(conn_window - 200)

    # the fast stream's data is fully consumed; the slow stream is not
    data_events[fast.id][0][1].consume(100)
    expect(client.remote_window).to eq(conn_window - 100)
    expect(fast.remote_window).to eq 100
    expect(slow.remote_window).to eq 0

    # the fast stream may send again while the slow one stays blocked
    fast.data("g" * 100, end_stream: false)
    expect(fast.buffered_amount).to eq 0
    expect(client.remote_window).to eq(conn_window - 200)

    # abandoning the slow stream's data returns its connection quota
    data_events[slow.id][0][1].discard
    expect(client.remote_window).to eq(conn_window - 100)
  end

  it "releases connection-level buffered data once other streams' quota is confirmed" do
    client, _server, _streams, data_events = manual_pair

    s1 = open_stream(client)
    s2 = open_stream(client)
    s1.data("a" * 40_000, end_stream: false)
    s2.data("b" * 40_000, end_stream: false)

    # connection window (65535) exhausted: the tail of s2's data is buffered
    expect(client.remote_window).to eq 0
    expect(client.buffered_amount).to eq(80_000 - DEFAULT_FLOW_WINDOW)

    # confirming everything received so far lets the buffered tail flow,
    # even though s2's own data has not been consumed at all
    data_events.each_value do |events|
      events.each { |_, receipt| receipt.consume(receipt.remaining) }
    end
    expect(client.buffered_amount).to eq 0
    expect(data_events[s1.id].flat_map(&:first).join.bytesize).to eq 40_000
    expect(data_events[s2.id].flat_map(&:first).join.bytesize).to eq 40_000
  end

  it "returns remaining connection quota on local reset and sends no stream-level WINDOW_UPDATE" do
    client, server, streams, data_events = manual_pair(server_settings: { settings_initial_window_size: 100 })

    stream = open_stream(client)
    stream.data("x" * 100, end_stream: false)
    expect(client.remote_window).to eq(DEFAULT_FLOW_WINDOW - 100)

    sent = []
    server.on(:frame_sent) { |frame| sent << frame }
    streams[stream.id].cancel

    # connection-level quota is returned, but nothing on the reset stream
    updates = sent.select { |frame| frame[:type] == :window_update }
    expect(updates.map { |frame| frame[:stream] }).to eq [0]
    expect(updates.sum { |frame| frame[:increment] }).to eq 100
    expect(client.remote_window).to eq DEFAULT_FLOW_WINDOW

    # confirming the receipt after the reset does not emit any more frames
    receipt = data_events[stream.id][0][1]
    expect { receipt.consume(100) }.not_to(change { sent.size })

    # the connection is not stuck: new streams can flow immediately
    other = open_stream(client)
    other.data("y" * 100, end_stream: false)
    expect(data_events[other.id].size).to eq 1
  end

  it "returns remaining connection quota when the peer resets the stream" do
    client, _server, _streams, data_events = manual_pair(server_settings: { settings_initial_window_size: 100 })

    stream = open_stream(client)
    stream.data("x" * 100, end_stream: false)
    stream.cancel

    expect(client.remote_window).to eq DEFAULT_FLOW_WINDOW
    expect(data_events[stream.id][0][1]).not_to be_nil
  end

  it "charges padding against the windows but only lets the application confirm payload bytes" do
    client, _server, _streams, data_events = manual_pair(server_settings: { settings_initial_window_size: 100 })

    stream = open_stream(client)
    stream.send(type: :data, flags: 0, payload: "abc", padding: 10)
    expect(stream.remote_window).to eq(100 - 13)
    expect(client.remote_window).to eq(DEFAULT_FLOW_WINDOW - 13)

    payload, receipt = data_events[stream.id][0]
    expect(payload).to eq "abc"
    expect(receipt.size).to eq 3
    expect(receipt.padding).to eq 10

    # padding bytes are invisible to the application and cannot be confirmed
    expect { receipt.consume(4) }.to raise_error(ArgumentError)

    # confirming the payload releases the padding quota automatically
    sent = []
    stream_updates = []
    client.on(:frame_received) do |frame|
      sent << frame if frame[:type] == :window_update && frame[:stream].zero?
      stream_updates << frame if frame[:type] == :window_update && frame[:stream] == stream.id
    end
    receipt.consume(3)
    expect(sent.sum { |frame| frame[:increment] }).to eq 13
    expect(stream_updates.sum { |frame| frame[:increment] }).to eq 13
    expect(stream.remote_window).to eq 100
    expect(client.remote_window).to eq DEFAULT_FLOW_WINDOW
  end

  it "rejects duplicate and out-of-range confirmations" do
    client, _server, _streams, data_events = manual_pair(server_settings: { settings_initial_window_size: 100 })

    stream = open_stream(client)
    stream.data("x" * 50, end_stream: false)
    receipt = data_events[stream.id][0][1]

    expect { receipt.consume(0) }.to raise_error(ArgumentError)
    expect { receipt.consume(-1) }.to raise_error(ArgumentError)
    expect { receipt.consume(51) }.to raise_error(ArgumentError)

    receipt.consume(20)
    expect(receipt.remaining).to eq 30
    expect { receipt.consume(31) }.to raise_error(ArgumentError)

    receipt.consume(30)
    expect(receipt).to be_complete
    expect { receipt.consume(1) }.to raise_error(ArgumentError)

    # discard after full consumption is a no-op; consume after discard fails
    stream.data("y" * 10, end_stream: false)
    other = data_events[stream.id][1][1]
    other.discard
    expect(other).to be_complete
    expect { other.consume(1) }.to raise_error(ArgumentError)
    expect(client.remote_window).to eq DEFAULT_FLOW_WINDOW
  end

  it "returns connection-level quota only once the peer ended the stream with END_STREAM" do
    client, server, _streams, data_events = manual_pair(server_settings: { settings_initial_window_size: 100 })

    stream = open_stream(client)
    stream.data("x" * 50, end_stream: true)

    sent = []
    server.on(:frame_sent) { |frame| sent << frame }
    data_events[stream.id][0][1].consume(50)

    updates = sent.select { |frame| frame[:type] == :window_update }
    expect(updates.map { |frame| frame[:stream] }).to eq [0]
    expect(updates.sum { |frame| frame[:increment] }).to eq 50
    expect(client.remote_window).to eq DEFAULT_FLOW_WINDOW
  end

  it "discard returns all remaining quota immediately" do
    client, _server, _streams, data_events = manual_pair(server_settings: { settings_initial_window_size: 100 })

    stream = open_stream(client)
    stream.data("x" * 80, end_stream: false)
    expect(stream.remote_window).to eq 20

    receipt = data_events[stream.id][0][1]
    receipt.consume(30)
    receipt.discard
    expect(receipt).to be_complete
    expect(stream.remote_window).to eq 100
    expect(client.remote_window).to eq DEFAULT_FLOW_WINDOW
  end

  it "keeps the default automatic window mode unchanged" do
    client = Client.new
    server = Server.new(settings_initial_window_size: 100)

    received = []
    server.on(:stream) do |stream|
      stream.on(:data) { |payload, receipt| received << [payload, receipt] }
    end
    connect(client, server)

    stream = open_stream(client)
    stream.data("x" * 60, end_stream: false)

    # no receipt is emitted and the window is returned automatically
    expect(received).to eq [["x" * 60, nil]]
    expect(stream.remote_window).to eq 100
    expect(client.remote_window).to eq DEFAULT_FLOW_WINDOW
  end
end
