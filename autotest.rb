#!/usr/bin/ruby

require 'colorize'

def run_test(itr)
  file = "/tmp/zksync-test-output-#{Process.pid}-#{itr}"
  output = `JAVA_HOME=/home/jonas/java/jdk1.8.0_171 mvn clean test -e -Dtest=com.acrescrypto.zksync.AllTests 1>#{file} 2>&1`
  return false unless $?.to_i == 0
  File.unlink(file)
  true
end

itr = 0
fails = 0

loop do
  itr += 1
  print "#{Time.now.strftime("%H:%M:%S")} Running iteration #{itr}... "
  start = Time.now
  passed = run_test(itr)
  duration = Time.now - start
  fails += 1 unless passed
  status = passed ? "PASS".green : "FAIL".red
  puts "#{status} #{duration.round(3)}s #{fails}/#{itr} #{(100.0*fails.to_f/itr).round(2)}%"
end
