# frozen_string_literal: true

module HTTP2
  # To decompress header blocks, a decoder only needs to maintain a
  # dynamic table as a decoding context.
  # No other state information is needed.
  module Header
    class EncodingContext
      include Error

      UPPER = /[[:upper:]]/.freeze

      # @private
      # Static table
      # - http://tools.ietf.org/html/draft-ietf-httpbis-header-compression-10#appendix-A
      STATIC_TABLE = [
        [":authority",                  ""],
        [":method",                     "GET"],
        [":method",                     "POST"],
        [":path",                       "/"],
        [":path",                       "/index.html"],
        [":scheme",                     "http"],
        [":scheme",                     "https"],
        [":status",                     "200"],
        [":status",                     "204"],
        [":status",                     "206"],
        [":status",                     "304"],
        [":status",                     "400"],
        [":status",                     "404"],
        [":status",                     "500"],
        ["accept-charset",              ""],
        ["accept-encoding",             "gzip, deflate"],
        ["accept-language",             ""],
        ["accept-ranges",               ""],
        ["accept",                      ""],
        ["access-control-allow-origin", ""],
        ["age",                         ""],
        ["allow",                       ""],
        ["authorization",               ""],
        ["cache-control",               ""],
        ["content-disposition",         ""],
        ["content-encoding",            ""],
        ["content-language",            ""],
        ["content-length",              ""],
        ["content-location",            ""],
        ["content-range",               ""],
        ["content-type",                ""],
        ["cookie",                      ""],
        ["date",                        ""],
        ["etag",                        ""],
        ["expect",                      ""],
        ["expires",                     ""],
        ["from",                        ""],
        ["host",                        ""],
        ["if-match",                    ""],
        ["if-modified-since",           ""],
        ["if-none-match",               ""],
        ["if-range",                    ""],
        ["if-unmodified-since",         ""],
        ["last-modified",               ""],
        ["link",                        ""],
        ["location",                    ""],
        ["max-forwards",                ""],
        ["proxy-authenticate",          ""],
        ["proxy-authorization",         ""],
        ["range",                       ""],
        ["referer",                     ""],
        ["refresh",                     ""],
        ["retry-after",                 ""],
        ["server",                      ""],
        ["set-cookie",                  ""],
        ["strict-transport-security",   ""],
        ["transfer-encoding",           ""],
        ["user-agent",                  ""],
        ["vary",                        ""],
        ["via",                         ""],
        ["www-authenticate",            ""]
      ].each(&:freeze).freeze

      STATIC_TABLE_BY_FIELD =
        STATIC_TABLE
        .each_with_object({})
        .with_index { |((field, value), hs), idx| (hs[field] ||= []) << [idx, value].freeze }
        .each_value(&:freeze)
        .freeze

      STATIC_TABLE_SIZE = STATIC_TABLE.size

      STATIC_ALL = %i[all static].freeze

      STATIC_NEVER = %i[never static].freeze

      FIELD_INDEXING_TYPES = {
        all: :all,
        static: :static,
        never: :never,
        index: :index,
        indexed: :index,
        incremental: :index,
        normal: :index,
        noindex: :noindex,
        no_index: :noindex,
        literal: :noindex,
        neverindexed: :neverindexed,
        never_indexed: :neverindexed
      }.freeze

      # Current table of header key-value pairs.
      attr_reader :table

      # Current encoding settings
      attr_reader :settings

      # Current table size in octets
      attr_reader :current_table_size

      # Initializes compression context with appropriate client/server
      # +settings+ and maximum size of the dynamic table.
      def initialize(settings = Settings.new)
        @table = []
        @table_by_field = Hash.new { |hs, k| hs[k] = [] }
        @unshifts = 0
        @settings = settings
        @limit = settings.table_size
        @_table_updated = false
        @current_table_size = 0
      end

      # Duplicates current compression context
      def dup
        other = EncodingContext.new(@settings)
        t = @table
        tbf = @table_by_field.transform_values(&:dup)
        unshifts = @unshifts
        l = @limit
        other.instance_eval do
          @table = t.dup # shallow copy
          @table_by_field = tbf
          @unshifts = unshifts
          @limit = l
        end
        other
      end

      # Finds an entry in current dynamic table by +index+.
      # Note that +index+ is zero-based in this module.
      #
      # If the +index+ is greater than the last index in the static table,
      # an entry in the dynamic table is dereferenced.
      #
      # If the +index+ is greater than the last header index, an error is raised.
      def dereference(index)
        # NOTE: index is zero-based in this module.
        return STATIC_TABLE[index] if index < STATIC_TABLE_SIZE

        idx = index - STATIC_TABLE_SIZE

        raise CompressionError, "Index too large" if idx >= @table.size

        @table[index - STATIC_TABLE_SIZE]
      end

      # Header Block Processing
      # - http://tools.ietf.org/html/draft-ietf-httpbis-header-compression-10#section-4.1
      def process(cmd)
        type = cmd[:type]
        name = cmd[:name]
        value = cmd[:value]

        case type
        when :changetablesize
          raise CompressionError, "tried to change table size after adding elements to table" if @_table_updated

          # we can receive multiple table size change commands inside a header frame. However,
          # we should blow up if we receive another frame where the new table size is bigger.
          table_size_updated = @limit != @settings.table_size

          raise CompressionError, "dynamic table size update exceed limit" if !table_size_updated && value > @limit

          self.table_size = value

          nil
        when :indexed
          # Indexed Representation
          # An _indexed representation_ entails the following actions:
          # o  The header field corresponding to the referenced entry in either
          # the static table or dynamic table is added to the decoded header
          # list.
          dereference(name)
        when :incremental, :noindex, :neverindexed
          # A _literal representation_ that is _not added_ to the dynamic table
          # entails the following action:
          # o  The header field is added to the decoded header list.

          # A _literal representation_ that is _added_ to the dynamic table
          # entails the following actions:
          # o  The header field is added to the decoded header list.
          # o  The header field is inserted at the beginning of the dynamic table.

          case name
          when Integer
            name, v = dereference(name)

            value ||= v
          when UPPER
            raise ProtocolError, "Invalid uppercase key: #{name}"
          end

          emit = [name, value]

          # add to table
          cmdsize = name.bytesize + value.bytesize + 32
          if type == :incremental && size_check?(cmdsize)
            @table.unshift(emit)
            @unshifts += 1
            @table_by_field[name].unshift([value, @unshifts])
            @current_table_size += cmdsize
            @_table_updated = true
          end

          emit
        else
          raise CompressionError, "Invalid type: #{type}"
        end
      end

      # Plan +headers+ compression.
      #
      # A field may carry +:index+, +:noindex+, or +:neverindexed+ as its
      # third entry. +index_policy+ is consulted exactly once while planning
      # each input field, so retrying the emitted buffer cannot select a
      # different representation. Never-indexed fields remain literal; their
      # names may still reference an existing static or dynamic entry.
      #
      # Choosing a non-indexing representation for a field does not remove an
      # entry that an earlier header block inserted. Existing entries remain
      # until normal HPACK eviction or a table-size/resizing update removes
      # them.
      def encode(headers, index: nil, index_policy: nil, indexing_policy: nil)
        headers.each do |field, value, field_indexing|
          # Literal header names MUST be translated to lowercase before
          # encoding and transmission.
          field = field.downcase if UPPER.match?(field)
          value = "/" if field == ":path" && value.empty?

          policy = index_policy || indexing_policy
          indexing = field_indexing
          indexing = policy.call(field, value) if policy
          indexing ||= index
          indexing = normalize_field_indexing(indexing)

          cmd = addcmd(field, value, indexing || @settings.index)
          process(cmd)
          yield cmd
        end
      end

      # Emits command for a +field+/+value+ header.
      # Prefer static table over dynamic table.
      # Prefer exact match over name-only match.
      #
      def addcmd(field, value, index_type = @settings.index)
        # @type var exact: Integer?
        exact = nil
        # @type var name_only: Integer?
        name_only = nil
        literal_only = %i[noindex neverindexed].include?(index_type)
        allow_static = STATIC_ALL.include?(index_type) ||
                        index_type == :never ||
                        literal_only
        allow_dynamic_exact = index_type == :all
        allow_dynamic_name = index_type == :all || literal_only

        if allow_static &&
           STATIC_TABLE_BY_FIELD.key?(field)
          STATIC_TABLE_BY_FIELD[field].each do |i, svalue|
            name_only ||= i
            if !literal_only && value == svalue
              exact = i
              break
            end
          end
        end

        if (allow_dynamic_exact || allow_dynamic_name) && (!exact || literal_only)
          field_entries = @table_by_field[field]

          field_entries&.each do |hvalue, unshift_id|
            abs_index = (@unshifts - unshift_id) + STATIC_TABLE_SIZE
            name_only ||= abs_index
            if allow_dynamic_exact && value == hvalue
              exact = abs_index
              break
            end
          end
        end

        if exact
          { name: exact, type: :indexed }
        else
          type = case index_type
                 when :neverindexed then :neverindexed
                 when :noindex, :never, :static then :noindex
                 else :incremental
                 end
          { name: name_only || field, value: value, type: type }
        end
      end

      # Alter dynamic table size.
      #  When the size is reduced, some headers might be evicted.
      def table_size=(size)
        @limit = size
        resize_table(0)
      end

      def listen_on_table
        yield
      ensure
        @_table_updated = false
      end

      private

      def normalize_field_indexing(indexing)
        return nil if indexing.nil?

        normalized = FIELD_INDEXING_TYPES.fetch(indexing) do
          raise ArgumentError, "unsupported indexing policy: #{indexing.inspect}"
        end

        normalized
      end

      def resize_table(cmdsize)
        return if @table.empty?

        while @current_table_size + cmdsize > @limit
          name, value = @table.pop
          @current_table_size -= name.bytesize + value.bytesize + 32

          field_arr = @table_by_field[name]
          field_arr.pop
          @table_by_field.delete(name) if field_arr.empty?

          break if @table.empty?

        end
      end

      # whether +cmd+ fits in the dynamic table.
      #
      # To keep the dynamic table size lower than or equal to @limit,
      # remove one or more entries at the end of the dynamic table.
      def size_check?(cmdsize)
        resize_table(cmdsize)
        cmdsize <= @limit
      end
    end
  end
end
