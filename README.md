Requirement
-----------
## python2.7+ && kazoo
https://github.com/python-zk/kazoo

Usage
-----

```
$ ./kafka-admin.py -h
usage: kafkaReassinger.py [-h] -z ZOOKEEPER [-t TOPICS] -m
                          {decommission,recommission,set-replica} [-r REPLICA]
                          [-b BROKERS] -f FILE_PATH

Simple Kafka Decommission/Recommission tool

optional arguments:
  -h, --help            show this help message and exit
  -z ZOOKEEPER, --zookeeper ZOOKEEPER
                        Zookeeper server hostname:port
  -t TOPICS, --topics TOPICS
                        Topics comma-separated list, will ressign all topics
                        if not specifying any topics
  -m {decommission,recommission,set-replica}, --mode {decommission,recommission,set-replica}
                        Mode
  -r REPLICA, --replica REPLICA
                        Set Replication Factor
  -b BROKERS, --brokers BROKERS
                        Brokers ID comma-separated list
  -f FILE_PATH, --file_path FILE_PATH
                        Output JSON file path
```

How does it work
----------------

### Step 1
- (Change replica)      ./kafka-admin.py -z zookeeper-host:2181 -t YYYYYYY -m set-replica -r 3 -f /tmp/XXXXXXXX.json
- (Decommission broker) ./kafka-admin.py -z zookeeper-host:2181 -m recommission -b 5 -f /tmp/XXXXXXXX.json
- (Recommission broker) ./kafka-admin.py -z zookeeper-host:2181 -m decommission -b 5 -f /tmp/XXXXXXXX.json

### Step 2
- (Execute the partition reassignment)  ./kafka-reassign-partitions.sh --zookeeper zookeeper-host:2181 --reassignment-json-file /tmp/XXXXXXXX.json --execute
- (Verify the progress)                 ./kafka-reassign-partitions.sh --zookeeper zookeeper-host:2181 --reassignment-json-file /tmp/XXXXXXXX.json --verify
- (Force leader election)               ./kafka-preferred-replica-election.sh --zookeeper zookeeper-host:2181
