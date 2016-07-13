#!/usr/bin/env ruby

require 'aws-sdk'

Aws.use_bundled_cert!

@kinesis = Aws::Kinesis::Client.new

def wait_for_active_state(stream_name)
  print "waiting for stream #{stream_name} to become active"
  STDOUT.flush
  while true
    print '.'
    STDOUT.flush
    status = @kinesis.describe_stream(stream_name: stream_name).stream_description.stream_status
    break if status == 'ACTIVE'
    sleep 10
  end
  puts "\nstream #{stream_name} is now active."
end

def shard_to_s(shard)
  s = shard
  "id: #{s.shard_id}, #{s.hash_key_range.starting_hash_key} - #{s.hash_key_range.ending_hash_key}"
end

def print_shards(shards)
  puts 'shards:'
  shards.each do |shard|
    puts "  #{shard_to_s shard}"
  end
end

def open_shards(stream_name)
  stream = @kinesis.describe_stream(stream_name: stream_name).stream_description
  # closed shards have an ending sequence number, dropping those.
  stream.shards.select { |s| s.sequence_number_range.ending_sequence_number.nil? }
end

def scale_up(stream_name)
  wait_for_active_state stream_name
  shards = open_shards stream_name
  puts "scaling up from #{shards.length} to #{shards.length * 2} shards."

  shards.each do |shard|
    puts "splitting shard #{shard_to_s shard}"
    new_starting_hash_key = ((shard.hash_key_range.starting_hash_key.to_i + shard.hash_key_range.ending_hash_key.to_i) / 2).to_s
    response = @kinesis.split_shard(
      stream_name: stream_name,
      shard_to_split: shard.shard_id,
      new_starting_hash_key: new_starting_hash_key)
    sleep 5
    wait_for_active_state stream_name
  end
end

def verify_mergable(merge_pairs)
  shards = merge_pairs.flatten
  if merge_pairs[-1].length == 1
    puts "cannot merge odd number of shards; got #{shards.length} shards."
    print_shards shards
    exit 1
  end

  all_consecutive = merge_pairs.all? do |left, right|
    left_end = left.hash_key_range.ending_hash_key.to_i
    right_start = right.hash_key_range.starting_hash_key.to_i
    if left_end + 1 == right_start
      true
    else
      puts 'found non-consecutive shards:'
      puts "  #{shard_to_s left}"
      puts "  #{shard_to_s right}"
      false
    end
  end

  unless all_consecutive
    puts "cannot merge shards, some are non-consecutive."
    print_shards shards
    exit 1
  end
end

def scale_down(stream_name)
  wait_for_active_state stream_name
  shards = open_shards stream_name
  shards.sort_by! { |s| s.hash_key_range.starting_hash_key.to_i }

  merge_pairs = shards.each_slice(2).to_a
  verify_mergable merge_pairs

  puts "scaling down from #{shards.length} to #{shards.length / 2} shards."

  puts "shard merging plan:"
  merge_pairs.each do |left, right|
    puts '  merge shard pair'
    puts "    #{shard_to_s left}"
    puts "    #{shard_to_s right}"
  end
  puts "executing merge plan."

  merge_pairs.each do |left, right|
    puts "merging shards #{left.shard_id} and #{right.shard_id}."
    @kinesis.merge_shards(
      stream_name: stream_name,
      shard_to_merge: left.shard_id,
      adjacent_shard_to_merge: right.shard_id
    )
    wait_for_active_state stream_name
  end

end

updown = ARGV.shift
stream = ARGV.shift

case updown
when 'up'
  scale_up stream
when 'down'
  scale_down stream
else
  puts "don't know how to do scale #{updown}, sorry. Please specify up or down."
  exit 1
end
