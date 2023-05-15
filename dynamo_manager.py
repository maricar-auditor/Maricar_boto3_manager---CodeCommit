import json
import sys

import boto3


_dyn_client = boto3.resource('dynamodb')


def parse_tabledef(conf_file):
    require_keys = [
        'table_name',
        'pk',
        'pkdef',
    ]
    with open(conf_file) as fh:
        conf = json.loads(fh.read())
        if require_keys == list(conf.keys()):
            return conf
        else:
            raise KeyError('Invalid configuration.')


def create_dynamo_table(table_name, pk, pkdef):
    table = _dyn_client.create_table(
        TableName=table_name,
        KeySchema=pk,
        AttributeDefinitions=pkdef,
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5,
        }
    )
    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
    return table


def get_dynamo_table(table_name):
    return _dyn_client.Table(table_name)


def delete_dynamo_table(table_name):
    table = get_dynamo_table(table_name)
    table.delete()
    table.wait_until_not_exists()
    return True


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(
        title='Commands',
    )

    # Create table subcommand
    sp_create_table = subparsers.add_parser(
        'create_table',
        help='Create a DynamoDB table',
    )
    sp_create_table.add_argument(
        'tabledef',
        help='Table definition file (JSON)',
    )
    sp_create_table.set_defaults(func=create_dynamo_table)

    # Delete table subcommand
    sp_delete_table = subparsers.add_parser(
        'delete_table',
        help='Delete a DynamoDB table',
    )
    sp_delete_table.add_argument(
        'tablename',
        help='Name of DynamoDB table to delete.',
    )
    sp_delete_table.set_defaults(func=delete_dynamo_table)

    # Execute subcommand function
    pargs = parser.parse_args()
    action = pargs.func.__name__ if hasattr(pargs, 'func') else ''
    if action == 'delete_dynamo_table':
        pargs.func(pargs.tablename)
    elif action == 'create_dynamo_table':
        conf = parse_tabledef(pargs.tabledef)
        pargs.func(**conf)
    else:
        print('Invalid/Missing command.')
        sys.exit(1)

    print('Done')
