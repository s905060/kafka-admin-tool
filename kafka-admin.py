#!/usr/bin/env python2.7

# Simple Kafka admin tool with minimal data movement
# Author: Jash Lee
#
# Usage:
# Step 1 -
# (Change replica)      ./kafkaReassinger.py -z zookeeper-host:2181 -t bdo_dummy -m set-replica -r 3 -f /tmp/XXXXXXXX.json
# (Decommission broker) ./kafkaReassinger.py -z zookeeper-host:2181 -m recommission -b 5 -f /tmp/XXXXXXXX.json
# (Recommission broker) ./kafkaReassinger.py -z zookeeper-host:2181 -m decommission -b 5 -f /tmp/XXXXXXXX.json
#
# Step 2 -
# (Execute the partition reassignment)  ./kafka-reassign-partitions.sh --zookeeper zookeeper-host:2181 --reassignment-json-file /tmp/XXXXXXXX.json --execute
# (Verify the progress)                 ./kafka-reassign-partitions.sh --zookeeper zookeeper-host:2181 --reassignment-json-file /tmp/XXXXXXXX.json --verify
# (Force leader election)               ./kafka-preferred-replica-election.sh --zookeeper zookeeper-host:2181

import json
import random
import argparse
from kazoo.client import KazooClient

class KafkaReassigner():

    def __init__(self, zookeeper_server, file_path):
        self.file_path = file_path
        self.zk = KazooClient(hosts=zookeeper_server, read_only=True)
        self.zk.start()

    def zk_query_get(self, znode):
        # Determine if a node exists
        if self.zk.exists(znode):
            data, stat = self.zk.get(znode)
            json_acceptable_string = data.replace("'", "\"")
            return json.loads(json_acceptable_string)

    def zk_query_list(self, znode):
        # Determine if a node exists
        if self.zk.exists(znode):
            # List the children
            data = self.zk.get_children(znode)
            return data

    def get_alive_broker_list(self):
        return self.zk_query_list('/brokers/ids')

    def get_topics_list(self):
        return self.zk_query_list('/brokers/topics')

    def get_topic_partitions_mapping(self, topic):
        topic = '/brokers/topics/' + topic
        return self.zk_query_get(topic)

    def get_topics_partition(self, topic=''):
        topic = '/brokers/topics/' + topic + '/partitions'
        return self.zk_query_list(topic)

    def write_json_file(self, data):
        with open(self.file_path, 'w') as fp:
            json.dump(data, fp, sort_keys=True, indent=4)

    def get_topic_replica(self, topic, partitions):
        total_replica = sum([len(partition_list) for partition_id, partition_list in partitions.iteritems()])
        total_partition = int(len(self.get_topic_partitions_mapping(topic)['partitions']))
        replica = int(total_replica/total_partition)
        return replica

    def replica_validator(self, replica):
        # Only allow maximum 3 replica
        if replica < 1:
            replica = 1
        elif replica > 3:
            replica = 3
        return int(replica)

    def decommission(self, topics, decommission_broker_id):
        newbrokerlist = [int(x) for x in self.get_alive_broker_list() if x not in decommission_broker_id]
        final_new_partition_list = []
        alive_brokers_count = int(len(self.get_alive_broker_list()))
        decommission_broker_count = int(len(decommission_broker_id))

        if len(topics) == 0:
            topics = self.get_topics_list()

        for topic in topics:
            data = self.get_topic_partitions_mapping(topic)
            partitions = data['partitions']
            replica = self.get_topic_replica(topic, partitions)

            if (alive_brokers_count - decommission_broker_count) < replica:
                raise ValueError("Replica is greater than alive brokers")

            replica = self.replica_validator(replica)

            for partition_id, partition_list in partitions.iteritems():
                new_partition_list = []
                partition_list = self.rebalancer(replica, partition_list)
                for partition in partition_list:
                    if str(partition) in decommission_broker_id:
                        new_replica_list = []
                        final_brokerlist = [int(x) for x in newbrokerlist if int(x) not in partition_list]
                        replica_id = int(random.choice(final_brokerlist))
                        while replica_id in new_replica_list:
                            replica_id = int(random.choice(final_brokerlist))
                        new_partition_list.append(replica_id)
                        random.shuffle(new_partition_list)
                    else:
                        new_partition_list.append(int(partition))
                tmp_dict = {"topic": topic, "partition": int(partition_id), "replicas": new_partition_list}
                final_new_partition_list.append(tmp_dict)

        self.generate_json(final_new_partition_list)

    def recommission(self, topics, recommission_broker_id):
        final_new_partition_list = []
        alive_brokers_count = int(len(self.get_alive_broker_list()))
        alive_broker_list = self.get_alive_broker_list()
        recommission_broker_count = int(len(recommission_broker_id))

        if len(topics) == 0:
            topics = self.get_topics_list()

        for topic in topics:
            partition_count = int(len(self.get_topics_partition(topic)))
            data = self.get_topic_partitions_mapping(topic)
            partitions = data['partitions']
            replica = self.get_topic_replica(topic, partitions)

            if (alive_brokers_count + recommission_broker_count) < replica:
                raise ValueError("Replica is greater than alive brokers")

            replica = self.replica_validator(replica)

            # Broker number greater than topic's partition 
            if (alive_brokers_count - len(recommission_broker_id)) > partition_count:
                for partition_id, partition_list in partitions.iteritems():
                    new_partition_list = []
                    partition_list = self.rebalancer(replica, partition_list)
                    for partition in partition_list:
                        new_partition_list.append(int(partition))
                    random.shuffle(new_partition_list)
                    tmp_dict = {"topic": topic, "partition": int(partition_id), "replicas": new_partition_list}
                    final_new_partition_list.append(tmp_dict)
            else:
                partition_counter = int((partition_count * replica / alive_brokers_count) * len(recommission_broker_id))
                for partition_id, partition_list in partitions.iteritems():
                    partition_list = self.rebalancer(replica, partition_list)
                    if partition_counter > 0:
                        new_partition_list = []
                        new_replica_list = []
                        newbrokerlist = [int(x) for x in alive_broker_list if int(x) not in partition_list]
                        replica_id = int(random.choice(newbrokerlist))
                        while replica_id in new_replica_list:
                            replica_id = int(random.choice(newbrokerlist))
                        partition_list[-1] = replica_id
                        random.shuffle(partition_list)
                        tmp_dict = {"topic": topic, "partition": int(partition_id), "replicas": partition_list}
                        final_new_partition_list.append(tmp_dict)
                        partition_counter -= 1
                    else:
                        tmp_dict = {"topic": topic, "partition": int(partition_id), "replicas": partition_list}
                        final_new_partition_list.append(tmp_dict)

        self.generate_json(final_new_partition_list)

    def rebalancer(self, replica, partition_list):
        alive_broker_list = self.get_alive_broker_list()

        if len(partition_list) == replica:
            return partition_list

        elif len(partition_list) < replica:
            # replica_range will be positive number
            replica_range = replica - len(partition_list)
            new_replica_list = []
            newbrokerlist = [int(x) for x in alive_broker_list if int(x) not in partition_list]
            for _ in xrange(replica_range):
                replica = int(random.choice(newbrokerlist))
                while replica in new_replica_list:
                    replica = int(random.choice(newbrokerlist))
                new_replica_list.append(replica)
            partition_list += new_replica_list
            random.shuffle(partition_list)
            return partition_list

        elif len(partition_list) > replica:
            # replica_range will be negative number
            replica_range = replica - len(partition_list)
            random.shuffle(partition_list)
            del partition_list[replica_range:]
            random.shuffle(partition_list)
            return partition_list

    def generate_json(self, final_new_partition_list):
        new_partition_json = {"version": 1, "partitions": final_new_partition_list}
        print new_partition_json
        self.write_json_file(new_partition_json)
        print 'Exported to ' + self.file_path
        self.zk.stop()

    def set_replication_factor(self, topics, new_replica):
        final_new_partition_list = []

        if len(topics) == 0:
            topics = self.get_topics_list()

        for topic in topics:
            data = self.get_topic_partitions_mapping(topic)
            partitions = data['partitions']

            for partition_id, partition_list in partitions.iteritems():
                partition_list = self.rebalancer(replica, partition_list)
                tmp_dict = {"topic": topic, "partition": int(partition_id) , "replicas": partition_list}
                final_new_partition_list.append(tmp_dict)

        self.generate_json(final_new_partition_list)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Simple Kafka Decommission/Recommission tool')
    parser.add_argument("-z", "--zookeeper", type=str, help="Zookeeper server hostname:port", required=True)
    parser.add_argument("-t", "--topics", type=str, help="Topics comma-separated list, will ressign all topics if not specifying any topics", default="")
    parser.add_argument("-m", "--mode", type=str, choices=["decommission", "recommission", "set-replica"], help="Mode", required=True)
    parser.add_argument("-r", "--replica", type=str, help="Set Replication Factor")
    parser.add_argument("-b", "--brokers", type=str, help="Brokers ID comma-separated list")
    parser.add_argument("-f", "--file_path", type=str, help="Output JSON file path", required=True)
    args = parser.parse_args()

    zookeeper_server = args.zookeeper
    if args.topics != '':
        topics = args.topics.split(',')
    else:
        topics = args.topics
    file_path = args.file_path
    mode = args.mode

    if mode == 'decommission' or mode == 'recommission':
        try:
            brokers = args.brokers.split(',')
        except:
            raise ValueError("Missing broker IDs")
        kafkareassigner = KafkaReassigner(zookeeper_server, file_path)
        if mode == 'decommission':
            kafkareassigner.decommission(topics, brokers)
        elif mode == 'recommission':
            kafkareassigner.recommission(topics, brokers)
    elif mode == 'set-replica':
        replica = int(args.replica)
        if replica is None or (replica > 3 or replica < 1) :
            raise ValueError("Missing replica parameter or replica greater than 3")
        kafkareassigner = KafkaReassigner(zookeeper_server, file_path)
        kafkareassigner.set_replication_factor(topics, replica)