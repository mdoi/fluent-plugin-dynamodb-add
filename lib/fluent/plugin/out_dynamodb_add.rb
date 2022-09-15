require 'aws-sdk-dynamodb'
require 'fluent/plugin/output'

class Fluent::Plugin::DynamodbAdd < Fluent::Plugin::Output
  Fluent::Plugin.register_output('dynamodb_add', self)

  helpers :event_emitter
  helpers :compat_parameters

  config_param :count_key, :string
  config_param :dynamo_count_key, :string
  config_param :table_name, :string
  config_param :use_iam_role, :bool, :default => false
  config_param :aws_key_id, :string, :default => nil
  config_param :aws_sec_key, :string, :default => nil
  config_param :region, :string, :default => nil
  config_param :endpoint, :string, :default => nil
  config_param :hash_key, :string, :default => nil
  config_param :hash_key_delimiter, :string, :default => ":"
  config_param :add_hash_key_prefix, :string, :default => nil
  config_param :range_key, :string, :default => nil
  config_param :set_timestamp, :string, :default => nil

  def initialize
    super
  end

  def configure(conf)
    compat_parameters_convert(conf)

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

    options = {}

    unless use_iam_role
      options[:access_key_id] = @aws_key_id
      options[:secret_access_key] = @aws_sec_key
    end

    options[:region] = region
    options[:endpoint] = endpoint

    client = Aws::DynamoDB::Client.new(options)

    resource = Aws::DynamoDB::Resource.new(client: client)
    @table = resource.table(table_name)

    @dynamo_hash_key = @table.key_schema.find{|e| e.key_type == "HASH" }.attribute_name
    @dynamo_range_key = @table.key_schema.find{|e| e.key_type == "RANGE" }&.attribute_name
  end

  def process(tag, es)
    es.each do |time, record|
      hash_key = create_key(record)
      next unless hash_key || record[@count_key]

      key = { @dynamo_hash_key => hash_key }

      if @range_key
        next unless record[@range_key]
        key[@dynamo_range_key] = record[@range_key]
      end

      @table.update_item({
        key: key,
        attribute_updates: {
          @dynamo_count_key => {
            value: record[@count_key],
            action: "ADD"
          },
        },
      })
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
