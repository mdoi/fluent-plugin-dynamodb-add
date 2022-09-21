require "bundler/gem_tasks"
require 'rake/testtask'

Rake::Task[:release].clear

Rake::TestTask.new(:test) do |test|
  test.libs << 'lib' << 'test'
  test.pattern = 'test/**/test_*.rb'
  test.verbose = true
end

task :default => [:build]

