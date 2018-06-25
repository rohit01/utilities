#!/usr/bin/env python
#
# Generate kafka reassignment partitions serially
#

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
    response = {}
    response['version'] = int(arguments["ver"])
    partitions = []
    broker_count = len(broker_list)

    for partition_no in range(start, start + partition_count):
        topic_details = {}
        topic_details['topic'] = arguments["topic"]
        topic_details['partition'] = partition_no
        replicas = []
        for rep_no in range(rep_factor):
            broker_index = (partition_no + (broker_count - 1) + rep_no + 1) % broker_count
            replicas.append(broker_list[broker_index])
        topic_details['replicas'] = replicas
        partitions.append(topic_details)
    response['partitions'] = partitions
    print json.dumps(response, indent=4, sort_keys=True)


def run():
    arguments = parse_options(OPTIONS, DESCRIPTION, USAGE, VERSION)
    generate_json(arguments)


if __name__ == '__main__':
    run()
