#!/usr/bin/env python
#
# Generate kafka reassignment partitions randomly
#

import math
import random
import json
import sys
import os
import optparse


__version__ = 0.1
VERSION = """Version: %s, Author: Rohit Gupta - @rohit01""" % __version__
DESCRIPTION = """Utility for generating kafka partition json for rebalancing"""
OPTIONS = {
    "brokers": "CSV broker numbers",
    "topic": "kafka topic name",
    "ver": "Version number. Default: 1",
    "partition": "Partition count of the topic",
    "start": "Partition starting number",
    "replication": "Replication factor. Default: 1",
}
DEFAULTS = {
    "ver": "1",
    "replication": "2",
    "start": 0,
}
USAGE = "%s [options]" % os.path.basename(__file__)


def parse_options(options, description=None, usage=None, version=None):
    parser = optparse.OptionParser(description=description, usage=usage,
                                   version=version)
    for keyname, description in options.items():
        longopt = '--%s' % keyname
        parser.add_option(longopt, dest=keyname, help=description)
    option_args, _ = parser.parse_args()
    arguments = {}
    for keyname in options.keys():
        arguments[keyname] = eval("option_args.%s" % keyname)
        if (arguments[keyname] is None) and (DEFAULTS.get(keyname, None) is not None):
            arguments[keyname] = DEFAULTS[keyname]
        if arguments[keyname] is None:
            print "Mandatory argument: --%s not provided. Use -h/--help for " \
                "more details." % keyname
            sys.exit(1)
    return arguments


def generate_json(arguments):
    broker_list = [int(i.strip()) for i in arguments['brokers'].split(",") if i.strip() != ""]
    rep_factor = int(arguments["replication"])
    partition_count = int(arguments["partition"])
    start = int(arguments["start"])
    broker_matrix = generate_broker_matrix(broker_list, rep_factor, partition_count)
    response = {}
    response['version'] = int(arguments["ver"])
    partitions = []

    for partition_no in range(start, start + partition_count):
        topic_details = {}
        topic_details['topic'] = arguments["topic"]
        topic_details['partition'] = partition_no
        replicas = []
        for rep_no in range(rep_factor):
            replicas.append(broker_matrix[rep_no][partition_no])
        topic_details['replicas'] = replicas
        partitions.append(topic_details)
    response['partitions'] = partitions
    print json.dumps(response, indent=4, sort_keys=True)


def generate_broker_matrix(broker_list, rep_factor, partition_count):
    multiplier = int(math.ceil(float(partition_count * rep_factor) / float(len(broker_list))))
    random.shuffle(broker_list)
    broker_complete_list = broker_list * multiplier
    broker_matrix = []
    start = 0
    for row_no in range(rep_factor):
        end = (row_no + 1) * partition_count
        broker_matrix.append(broker_complete_list[start:end])
        start = end
    # Shuffle
    for row_no in range(rep_factor):
        unique = False
        while not unique:
            random.shuffle(broker_matrix[row_no])
            # Assume all shuffled rows are unique until proven wrong
            unique = True
            for col_index in range(partition_count):
                items_in_col = []
                for row_index in range(row_no + 1):
                    if broker_matrix[row_index][col_index] in items_in_col:
                        # There is duplicate, re-shuffle
                        unique = False
                        break
                    else:
                        items_in_col.append(broker_matrix[row_index][col_index])
                if not unique:
                    break
    return broker_matrix


def run():
    arguments = parse_options(OPTIONS, DESCRIPTION, USAGE, VERSION)
    generate_json(arguments)


if __name__ == '__main__':
    run()
