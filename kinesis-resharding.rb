#!/usr/bin/env ruby

require 'aws-sdk'

Aws.use_bundled_cert!
def has_child_shard(shards,shard_id)
  shards.select{ |s| s[:parent_shard_id] == shard_id}.length > 0
end

kinesis = Aws::Kinesis::Client.new

stream_name = ARGV.shift
updown = ARGV.shift

stream = kinesis.describe_stream(:stream_name => stream_name)[:stream_description]
cur_shards = stream[:shards]

if updown == 'up'
  puts "Scaling up, double shards"
  cur_shards.select{ |s| !has_child_shard(cur_shards, s[:shard_id]) }.each do |shard|
    puts "splitting shard #{shard[:shard_id]}"
    new_starting_hash_key = ((shard[:hash_key_range][:starting_hash_key].to_i + shard[:hash_key_range][:ending_hash_key].to_i) / 2).to_s
    response = kinesis.split_shard(:stream_name => stream_name, :shard_to_split => shard[:shard_id], :new_starting_hash_key => new_starting_hash_key)
    while true
      sleep 10
      break if kinesis.describe_stream(:stream_name => stream_name)[:stream_description][:stream_status] == 'ACTIVE'
    end
  end
end
