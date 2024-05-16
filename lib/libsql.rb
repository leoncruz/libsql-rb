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

    def execute(query_string)
      body = {
        requests: [
          { type: :execute, stmt: { sql: query_string } },
          { type: :close }
        ]
      }

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

  class QueryResult
    Row = Struct.new(:column, :value)

    def initialize(response)
      @results = response.results
    end

    def rows
      @results.first["response"]["result"]["rows"].map do |register|
        register.map.with_index do |r, index|
          Row.new(cols[index]["name"], r["value"])
        end
      end.flatten
    end

    private

      def cols
        @results.first["response"]["result"]["cols"]
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
