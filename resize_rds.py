from tracemalloc import Snapshot
import boto3
import os
import boto3
import yaml
import argparse
import datetime
import time as tm
import logging
import json
from botocore.exceptions import ClientError

# Set config file path
CONF_PATH_ENV_KEY = "resize_conf.yml"


# For logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler('rds_utility_tracker.log')
stream_handler = logging.StreamHandler()
stream_formatter = logging.Formatter(
    '%(asctime)-15s p%(process)s {%(pathname)s:%(lineno)d} %(levelname)-8s %(funcName)s  %(message)s')
file_formatter = logging.Formatter(json.dumps(
    {'time': '%(asctime)s', 'level': '%(levelname)s', 'function name ': '%(funcName)s', 'process': 'p%(process)s', 'line no': '%(lineno)d', 'message': '%(message)s'}))
file_handler.setFormatter(file_formatter)
stream_handler.setFormatter(stream_formatter)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

# Function use for the database fetching based on the tags
def rds_db_instance(tags):
    client = boto3.client('rds')
    logging.info(f'Connected with rds resources')
    resource = client.describe_db_instances()
    databases = []
    logging.info(f'Start fetching the database based on the tags .......')
    for i in resource['DBInstances']:
        try:
            if tags['tags'] in i["TagList"]:
                databases.append(i['DBInstanceIdentifier'])
            logging.info(
                f'Fetched databases based on the tags are {databases}')

        except:
            logging.error(f'No database found based on the tags')

    return databases

# Function for the database modification
def rds_db_modification(modification):
    databases = rds_db_instance(modification)
    client = boto3.client('rds')
    new = modification['modification']
    for db in databases:
        try:
            response = client.modify_db_instance(
                DBInstanceIdentifier=db,
                AllocatedStorage=new['allocatedStorage'],
                DBInstanceClass=new['dbInstanceClass'],
                ApplyImmediately=True,
            )
            logging.info(f'Modification of {db} is done')
        except:
            logging.info(f'Modification of {db} is failed')


#function for resize the redis
def resize_redis(modification):
    client = boto3.client("elasticache")
    redis = client.describe_replication_groups()
    clustername = []

    for i in redis['ReplicationGroups']:
        taglist = client.list_tags_for_resource(ResourceName=i['ARN'])
        for tag in taglist['TagList']:
            if modification['tags'] == tag:
                clustername.append(i['ReplicationGroupId'])
                logging.info('{} redis is found based on the tags {}'.format(i['ReplicationGroupId'],tag))

    new = modification['redisModification']
    for redis in clustername:
        response = client.modify_replication_group(
        ReplicationGroupId=redis,
        ApplyImmediately=True, 
        CacheNodeType=new['CacheNodeType']
        )
        logging.info(f'Modification of {redis} redis done.')


# Check the property file
def _getProperty(property_file_path):

    try:
        load_property = open(property_file_path)
        parse_yaml = yaml.load(load_property, Loader=yaml.FullLoader)

        logging.info(f'configuration file path found {property_file_path}')

        return parse_yaml

    except FileNotFoundError:
        logging.exception(
            f"unable to find {property_file_path}. Please mention correct property file path.")

    return None


# main function
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    file = _getProperty(CONF_PATH_ENV_KEY)
    # rds_db_modification(file)
    resize_redis(file)
