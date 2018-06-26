#!/usr/bin/ruby

require 'colorize'

def run_test(itr)
  output = `mvn clean test -e -Dtest=com.acrescrypto.zksync.AllTests 2>&1`
  return true if $?.to_i == 0
  IO.write("/tmp/zksync-failure-output-#{itr}", output)
  false
end

itr = 0
loop do
  itr += 1
  print "Running iteration #{itr}... "
  start = Time.now
  passed = run_test(itr)
  duration = Time.now - start
  status = passed ? "PASS".green : "FAIL".red
  puts "#{status} #{duration.round(3)}s"
end
