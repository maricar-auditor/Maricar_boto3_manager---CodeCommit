#!/usr/bin/env python
import logging
import random
import uuid

import argparse
from datetime import datetime
from decimal import Decimal
from pathlib import Path, PosixPath
import boto3
from botocore.exceptions import ClientError

def list_log_groups(group_name=None, region_name=None):
    cwlogs = boto3.client('logs', region_name=region_name)
    params = {
        'logGroupNamePrefix': group_name,
    } if group_name else {}
    res = cwlogs.describe_log_groups(**params)
    return res['logGroups']

def list_log_group_streams(group_name, stream_name=None, region_name=None):
    cwlogs = boto3.client('logs', region_name=region_name)
    params = {
        'logGroupName': group_name,
    } if group_name else {}
    if stream_name:
        params['logStreamNamePrefix'] = stream_name
    res = cwlogs.describe_log_streams(**params)
    return res['logStreams']

def filter_log_events(
    group_name, filter_pat,
    start=None, stop=None,
    region_name=None
):
    cwlogs = boto3.client('logs', region_name=region_name)
    params = {
        'logGroupName': group_name,
        'filterPattern': filter_pat,
    }
    if start:
        params['startTime'] = start
    if stop:
        params['endTime'] = stop
    res = cwlogs.filter_log_events(**params)
    return res['events']

if __name__ == '__main__':    
    parser = argparse.ArgumentParser(description='AWS SNS Operation')
    subparsers=parser.add_subparsers(title="Commands", dest="command")
    
    #List Log Groups
    sp_list_log_groups=subparsers.add_parser('list_log_groups',  help='Creates a list of log groups')
    sp_list_log_groups.add_argument('--group_name', type=str, help='Group name')
    sp_list_log_groups.add_argument('--region', type=str, help='Region')

    #List Log Group Streams
    sp_list_log_group_streams=subparsers.add_parser('list_log_group_streams',  help='Creates a list of log groupstreams')
    sp_list_log_group_streams.add_argument('group_name', type=str, help='Group name')
    sp_list_log_group_streams.add_argument('--stream_name', type=str, help='Name of stream')
    sp_list_log_group_streams.add_argument('--region_name', type=str, help='Name of region')

    #Filter Log Events
    sp_filter_log_events=subparsers.add_parser('filter_log_events', help='Filters log events')
    sp_filter_log_events.add_argument('group_name', type=str, help='Group name')
    sp_filter_log_events.add_argument('filter_pat', type=str, help='Filter Pattern')
    sp_filter_log_events.add_argument('--start', type=str, help='Start')
    sp_filter_log_events.add_argument('--stop', type=str, help='Stop')
    sp_filter_log_events.add_argument('--region_name', type=str, help='The name of the region')

    args = parser.parse_args()

if args.command == 'list_log_groups':
    out=list_log_groups(args.group_name, args.region_name)
    print(out)
elif args.command == "list_log_group_streams":
    out=list_log_group_streams(args.group_name, args.stream_name, args.region_name)
    print(out)
elif args.command == "filter_log_events":
     filter_log_events(args.group_name, args.filter_pat, args.start, args.stop, args.region_name)


    