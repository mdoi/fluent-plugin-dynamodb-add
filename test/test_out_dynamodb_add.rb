require 'fluent/test'
require 'fluent/plugin/out_dynamodb_add'

class DynamodbAddTest < Test::Unit::TestCase
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
    Fluent::Test::OutputTestDriver.new(Fluent::DynamodbAdd) do
      def write(chunk)
        chunk.read
      end
    end.configure(conf)
  end

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
end

