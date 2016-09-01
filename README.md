# kinesis-tools
Tools for working with AWS Kinesis streams

### Owner
[Shared Services](https://github.com/orgs/AutoScout24/teams/sharedservices)

### kinesis-tail
"tail -f" for AWS Kinesis streams

#### Usage
```
./kinesis-tail.rb streamName
```

### kinesis-resharding
- Easy upscaling of AWS Kinesis streams
- Doubles the amount of shards

#### Usage
To double the number of shards, use
```
./kinesis-resharding.rb up streamName
```
or to halve the number of shards, use
```
./kinesis-resharding.rb down streamName
```
