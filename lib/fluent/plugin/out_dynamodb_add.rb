module Fluent
  class DynamodbAdd < Fluent::Output
    Fluent::Plugin.register_output('dynamodb_add', self)

    unless method_defined?(:log)
      define_method(:log) { $log }
    end

    config_param :count_key, :string
    config_param :dynamo_count_key, :string
    config_param :table_name, :string
    config_param :use_iam_role, :bool, :default => false
    config_param :aws_key_id, :string, :default => nil
    config_param :aws_sec_key, :string, :default => nil
    config_param :endpoint, :string, :default => nil
    config_param :hash_key, :string, :default => nil
    config_param :hash_key_delimiter, :string, :default => ":"
    config_param :add_hash_key_prefix, :string, :default => nil
    config_param :range_key, :string, :default => nil
    config_param :set_timestamp, :string, :default => nil

    def initialize
      super
      require 'aws-sdk'
    end

    def configure(conf)
      super

      unless use_iam_role
        [:aws_key_id, :aws_sec_key].each do |name|
          unless self.instance_variable_get("@#{name}")
            raise ConfigError, "'#{name}' is required"
          end
        end
      end
      @hash_key = hash_key.split(/\s*,\s*/)
    end

    def start
      super
      if use_iam_role
        AWS.config(:credential_provider => AWS::Core::CredentialProviders::EC2Provider.new)
      else
        AWS.config(:access_key_id => @aws_key_id, :secret_access_key => @aws_sec_key)
      end

      AWS.config(:dynamo_db_endpoint => @endpoint) if @endpoint

      @dynamo_db = AWS::DynamoDB.new
      @table = @dynamo_db.tables[table_name]
      @table.load_schema
    end

    def emit(tag, es, chain)
      chain.next
      es.each do |time, record|
        hash_key = create_key(record)
        next unless hash_key || record[@count_key]

        if @range_key
          next unless record[@range_key]
          item = @table.items[hash_key, record[@range_key]]
        else
          item = @table.items[hash_key]
        end
        item.attributes.update {|u|
          u.add @dynamo_count_key => record[@count_key]
          if @set_timestamp
            u.set @set_timestamp => Time.now.to_i
          end
        }
      end
    end

    private
    def create_key(record)
      key_array = []
      key_array << @add_hash_key_prefix if @add_hash_key_prefix
      @hash_key.each do |h|
        return nil unless record[h]
        key_array << record[h]
      end
      key_array.join(@hash_key_delimiter)
    end
  end
end
