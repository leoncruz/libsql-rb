require "json"
require "net/http"

module Libsql
  class Client
    STMT_ENDPOINT = "/v2/pipeline".freeze

    def initialize(params = {})
      @url = params[:url]
      @host = params[:host]
      @port = params[:port]

      _build_uri
    end

    def execute(query_string, args = nil)
      argument_builder = case args
      in Array
        PositionalArgBuilder.new(*args)
      in Hash
        NamedArgBuilder.new(**args)
      in NilClass
        EmptyBuilder.new
      else
        raise NotImplementedError
      end

      if argument_builder.empty?
        body = {
          requests: [
            { type: :execute, stmt: { sql: query_string } },
            { type: :close }
          ]
        }
      else
        body = {
          requests: [
            { type: :execute, stmt: { sql: query_string, argument_builder.key => argument_builder.build } },
            { type: :close }
          ]
        }
      end

      response = http_client.post(STMT_ENDPOINT, body)

      raise StandardError, response.results.first["error"]["message"] if response.failure?

      QueryResult.new(response)
    end

    private

      def http_client
        @http_client ||= HTTPClient.new(@uri)
      end

      def _build_uri
        unless @url
          @uri = URI("#{@host}:#{@port}")
        else
          @uri = URI(@url)
        end
      end
  end

  class EmptyBuilder
    def empty? = true

    def key = :no_key
  end

  class PositionalArgBuilder
    attr_reader :args

    def initialize(*args)
      @args = args
    end

    def empty? = @args.empty?

    def key = :args

    def build
      args.reduce([]) do |acc, value|
        type = case value
        in Integer
          "integer"
        in NilClass
          "null"
        else
          "text"
        end

        acc << { type:, value: }
      end
    end
  end

  class NamedArgBuilder
    attr_reader :kwargs

    def initialize(**kwargs)
      @kwargs = kwargs
    end

    def empty? = @kwargs.empty?

    def key = :named_args

    def build
      kwargs.reduce([]) do |acc, (key, value)|
        type = case value
        in Integer
          "integer"
        in NilClass
          "null"
        else
          "text"
        end

        acc << { name: key, value: { type:, value: } }
      end
    end
  end

  class QueryResult
    def initialize(response)
      @results = response.results
    end

    def rows
      @results.first["response"]["result"]["rows"].map do |register|
        row = Row.new

        register.map.with_index do |r, index|
          row.set(cols[index]["name"], r["value"])
        end

        row
      end.flatten
    end

    private

      def cols
        @results.first["response"]["result"]["cols"]
      end

      class Row
        def set(column, value)
          instance_variable_set :"@#{column}", value

          self.class.class_eval do
            define_method column.to_sym, -> { value }
          end
        end
      end
  end

  class HTTPClient
    def initialize(base_uri)
      @base_uri = base_uri
    end

    def get(endpoint)
      uri = base_uri.merge endpoint

      request = Net::HTTP::Get.new(uri)

      execute(uri, request)
    end

    def post(endpoint, body = {})
      uri = base_uri.merge endpoint

      request = Net::HTTP::Post.new(uri)
      request["Content-Type"] = "application/json"

      request.body = JSON.generate(body)

      execute(uri, request)
    end

    private

      attr_accessor :base_uri

      def execute(uri, request)
        use_ssl = uri.scheme == "https"
        request["Content-Type"] = "application/json"

        resp = Net::HTTP.start(uri.hostname, uri.port, use_ssl:) do |http|
          http.request(request)
        end

        Response.new(resp)
      end

      class Response
        attr_reader :status_code

        def initialize(response)
          @json_response = response.body
          @status_code = response.code
        end

        def body
          @body ||= JSON.parse(@json_response)
        end

        def success?
          body["results"].all? { |r| r["type"] == "ok" }
        end

        def failure?
          body["results"].any? { |r| r["type"] == "error" }
        end

        def results
          body["results"]
        end
      end
  end
end
