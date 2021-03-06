# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name          = "fluent-plugin-dynamodb-add"
  spec.authors       = ["Masayuki DOI"]
  spec.email         = ["dotquasar@gmail.com"]
  spec.version       = '0.1.2'
  spec.licenses      = ['MIT']
  spec.description   = "Amazon DynamoDB atomic add plugin"
  spec.summary       = spec.description
  spec.homepage      = "https://github.com/mdoi/fluent-plugin-dynamodb-add"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.7"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_dependency "fluentd", "~> 0.10.0"
  spec.add_dependency "aws-sdk", ">= 1.56.0", "< 2.0.0"
end
