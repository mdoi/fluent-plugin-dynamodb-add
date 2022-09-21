require 'helper'

require 'fluent/plugin/out_dynamodb_add'
require 'aws-sdk-dynamodb'

class DynamodbAddTest < Test::Unit::TestCase
  include Fluent::Test::Helpers

  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    count_key test_count_key
    dynamo_count_key test_dynamo_count_key
    table_name test_table_name
    use_iam_role false
    aws_key_id test_aws_key_id
    aws_sec_key test_aws_sec_key
    endpoint https://test_endpoint
    hash_key test_hash_key1,test_hash_key2
    hash_key_delimiter :
    add_hash_key_prefix 3
    range_key test_range_key
    ]

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::DynamodbAdd).configure(conf)
  end

  sub_test_case 'confguration' do
    def test_configure_not_use_iam_role
      d = create_driver
      assert_equal 'test_count_key', d.instance.count_key
      assert_equal 'test_dynamo_count_key', d.instance.dynamo_count_key
      assert_equal 'test_table_name', d.instance.table_name
      assert_equal false, d.instance.use_iam_role
      assert_equal 'test_aws_key_id', d.instance.aws_key_id
      assert_equal 'test_aws_sec_key', d.instance.aws_sec_key
      assert_equal 'https://test_endpoint', d.instance.endpoint
      assert_equal ['test_hash_key1','test_hash_key2'], d.instance.hash_key
      assert_equal ':', d.instance.hash_key_delimiter
      assert_equal '3', d.instance.add_hash_key_prefix
      assert_equal 'test_range_key', d.instance.range_key
    end

    def test_configure_use_iam_role
      conf = CONFIG.clone
      conf.gsub!(/use_iam_role\sfalse/, "use_iam_role true")
      conf.gsub!(/aws_key_id\stest_aws_key_id/, "")
      conf.gsub!(/aws_sec_key\stest_aws_sec_key/, "")

      d = create_driver(conf)
      assert_equal 'test_count_key', d.instance.count_key
      assert_equal 'test_dynamo_count_key', d.instance.dynamo_count_key
      assert_equal 'test_table_name', d.instance.table_name
      assert_equal true, d.instance.use_iam_role
      assert_equal nil, d.instance.aws_key_id
      assert_equal nil, d.instance.aws_sec_key
      assert_equal 'https://test_endpoint', d.instance.endpoint
      assert_equal ['test_hash_key1','test_hash_key2'], d.instance.hash_key
      assert_equal ':', d.instance.hash_key_delimiter
      assert_equal '3', d.instance.add_hash_key_prefix
      assert_equal 'test_range_key', d.instance.range_key
    end

    def test_configure_not_use_iam_role_and_not_set_range_key
      conf = CONFIG.clone
      conf.gsub!(/range_key\stest_range_key/, "")

      d = create_driver(conf)
      assert_equal 'test_count_key', d.instance.count_key
      assert_equal 'test_dynamo_count_key', d.instance.dynamo_count_key
      assert_equal 'test_table_name', d.instance.table_name
      assert_equal false, d.instance.use_iam_role
      assert_equal 'test_aws_key_id', d.instance.aws_key_id
      assert_equal 'test_aws_sec_key', d.instance.aws_sec_key
      assert_equal 'https://test_endpoint', d.instance.endpoint
      assert_equal ['test_hash_key1','test_hash_key2'], d.instance.hash_key
      assert_equal ':', d.instance.hash_key_delimiter
      assert_equal '3', d.instance.add_hash_key_prefix
      assert_equal nil, d.instance.range_key
    end

    def test_configure_set_timestamp
      conf = CONFIG.clone
      conf << " set_timestamp last_updated_at"

      d = create_driver(conf)
      assert_equal 'test_count_key', d.instance.count_key
      assert_equal 'test_dynamo_count_key', d.instance.dynamo_count_key
      assert_equal 'test_table_name', d.instance.table_name
      assert_equal false, d.instance.use_iam_role
      assert_equal 'test_aws_key_id', d.instance.aws_key_id
      assert_equal 'test_aws_sec_key', d.instance.aws_sec_key
      assert_equal 'https://test_endpoint', d.instance.endpoint
      assert_equal ['test_hash_key1','test_hash_key2'], d.instance.hash_key
      assert_equal ':', d.instance.hash_key_delimiter
      assert_equal '3', d.instance.add_hash_key_prefix
      assert_equal 'test_range_key', d.instance.range_key
      assert_equal 'last_updated_at', d.instance.set_timestamp
    end
  end


  def test_count_with_range_key_table
    table_name = 'sample_table'

    create_table(dynamodb_client, table_with_range_key(table_name))

    d = create_driver(
      <<~EOS
      count_key count
      hash_key project_id

      table_name #{table_name}
      range_key time
      dynamo_count_key count

      endpoint http://localhost:8000
      region ap-northeast-1
      aws_key_id dummy
      aws_sec_key dummy
      EOS
    )

    resource = Aws::DynamoDB::Resource.new(client: dynamodb_client)
    @table = resource.table(table_name)

    time = 1000

    d.run(default_tag: 'test') do
      d.feed(event_time('2022-09-01 10:00:00 UTC'), {'count' => 1, 'project_id' => 1, 'time' => time})
      d.feed(event_time('2022-09-01 10:00:00 UTC'), {'count' => 2, 'project_id' => 1, 'time' => time})
      d.feed(event_time('2022-09-01 10:00:00 UTC'), {'count' => 4, 'project_id' => 1, 'time' => 2000})
    end

    item = @table.get_item(key: { 'Id': '1', 'ViewTimestamp': time.to_i}).item

    assert_equal 3, item['count']

    d.run(default_tag: 'test') do
      d.feed(event_time('2022-09-01 10:00:00 UTC'), {'count' =>  8, 'project_id' => 1, 'time' => time})
      d.feed(event_time('2022-09-01 10:00:00 UTC'), {'count' => 16, 'project_id' => 1, 'time' => time})
      d.feed(event_time('2022-09-01 10:00:00 UTC'), {'count' => 32, 'project_id' => 1, 'time' => 2000})
    end

    item = @table.get_item(key: { 'Id': '1', 'ViewTimestamp': time.to_i}).item

    assert_equal 27, item['count']

    delete_table(dynamodb_client, table_name)
  end

  def test_count_with_no_range_key_table
    table_name = 'sample_table'

    create_table(dynamodb_client, table_without_range_key(table_name))

    d = create_driver(
      <<~EOS
      count_key count
      hash_key project_id

      table_name #{table_name}
      dynamo_count_key count

      endpoint http://localhost:8000
      region ap-northeast-1
      aws_key_id dummy
      aws_sec_key dummy
      EOS
    )

    resource = Aws::DynamoDB::Resource.new(client: dynamodb_client)
    @table = resource.table(table_name)

    d.run(default_tag: 'test') do
      d.feed(event_time('2022-09-01 10:00:00 UTC'), {'count' => 1, 'project_id' => 1})
      d.feed(event_time('2022-09-01 10:00:00 UTC'), {'count' => 2, 'project_id' => 1})
    end

    item = @table.get_item(key: { 'Id': '1'}).item

    assert_equal 3, item['count']

    d.run(default_tag: 'test') do
      d.feed(event_time('2022-09-01 10:00:00 UTC'), {'count' => 4, 'project_id' => 1})
      d.feed(event_time('2022-09-01 10:00:00 UTC'), {'count' => 8, 'project_id' => 1})
    end
    
    item = @table.get_item(key: { 'Id': '1'}).item

    assert_equal 15, item['count']

    delete_table(dynamodb_client, table_name)
  end

  private

  def create_table(dynamodb_client, table_definition)
    response = dynamodb_client.create_table(table_definition)
    response.table_description.table_status
  rescue StandardError => e
    binding.irb
  end

  def delete_table(dynamodb_client, table_name)
    dynamodb_client.delete_table(
      table_name: table_name
    )
  end

  def table_without_range_key(table_name)
    table_definition = {
      table_name: table_name, 
      key_schema: [
        {
          attribute_name: 'Id',
          key_type: 'HASH'  # Partition key.
        },
      ],
      attribute_definitions: [
        {
          attribute_name: 'Id',
          attribute_type: 'S'
        },
      ],
      billing_mode: "PAY_PER_REQUEST",
    }
  end

  def table_with_range_key(table_name)
    table_definition = {
      table_name: table_name,
      key_schema: [
        {
          attribute_name: 'Id',
          key_type: 'HASH'  # Partition key.
        },
        {
          attribute_name: 'ViewTimestamp',
          key_type: 'RANGE' # Sort key.
        }
      ],
      attribute_definitions: [
        {
          attribute_name: 'Id',
          attribute_type: 'S'
        },
        {
          attribute_name: 'ViewTimestamp',
          attribute_type: 'N'
        }
      ],
      billing_mode: "PAY_PER_REQUEST",
    }
  end


  def dynamodb_client
    @client ||= Aws::DynamoDB::Client.new({
      access_key_id: 'dummy',
      secret_access_key: 'dummy',
      endpoint: 'http://localhost:8000',
      region: 'ap-northeast-1'
    })
  end
end
