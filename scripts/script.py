#!/usr/bin/env python3

from ctypes import resize
import sys, os, argparse, logging, yaml, json
import json_log_formatter
import pathlib
import boto3
from botocore.exceptions import ClientError
SCRIPT_PATH = pathlib.Path(__file__).parent.resolve()
sys.path.insert(1, f'{SCRIPT_PATH}/../lib')
import load_yaml_config
from otawslibs import generate_aws_session , aws_resource_tag_factory , aws_ec2_actions_factory , aws_rds_actions_factory

SCHEULE_ACTION_ENV_KEY = "SCHEDULE_ACTION"
CONF_PATH_ENV_KEY = "CONF_PATH"
LOG_PATH = "./logs/aws-resource-scheduler.log"

FORMATTER = json_log_formatter.VerboseJSONFormatter()
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

FILE_HANDLER = logging.FileHandler(LOG_PATH)
STREAM_HANDLER = logging.StreamHandler(sys.stdout)

FILE_HANDLER.setFormatter(FORMATTER)
STREAM_HANDLER.setFormatter(FORMATTER)

LOGGER.addHandler(FILE_HANDLER)
LOGGER.addHandler(STREAM_HANDLER)


def fetch_instance(service,region,tags):
    print(region,tags)
    print(region[0], tags)
    client = boto3.client(service, region_name=region[0])
    resource = client.describe_instances()
    print(resource['Reservations'][1]['Instances'][0]['Tags'])
    instances_id = []
    for i in resource['Reservations']:
            # print(i)
            for j in i['Instances']:
                print(j)
                # if tags in j['Tags']:
                #     print(tags)
                #     print(i)
                #     instances_id.append(j['InstanceId'])
    return instances_id      

def fetch_rds_db_instance(service,region,tags):
    # print(region,tags)
    # print(region[0], tags)
    client = boto3.client(service, region_name=region[0])
    resource = client.describe_db_instances()
    databases = []
    for i in resource['DBInstances']:
            if tags in i["TagList"]:
                databases.append(i['DBInstanceIdentifier'])
    return databases       

def rds_db_modification(service,region,tags,properties):
    databases = fetch_rds_db_instance(service,region,tags)
    # print(databases)
    # modification = _getProperty(CONF_PATH_ENV_KEY)
    client = boto3.client(service,region_name=region[0])
    logging.info(f'Start modifying the databases.....')
    # print(properties['services']['rds']['resize_params']['allocatedStorage'])
    for db in databases:
        try:
            response = client.modify_db_instance(
                DBInstanceIdentifier=db,
                AllocatedStorage=properties['services']['rds']['resize_params']['allocatedStorage'],
                DBInstanceClass=properties['services']['rds']['resize_params']['dbInstanceClass'],
                ApplyImmediately=True,
            )
            logging.info(f'Modification of {db} is done')
        except:
            logging.info(f'Modification of {db} is failed')

def fetch_redis_cluster(service,region,tags):
    client = boto3.client(service, region_name=region[0])
    redis = client.describe_replication_groups()
    clustername = []

    for i in redis['ReplicationGroups']:
        taglist = client.list_tags_for_resource(ResourceName=i['ARN'])
        for tag in taglist['TagList']:
            if tags == tag:
                clustername.append(i['ReplicationGroupId'])
                logging.info('{} redis is found based on the tags {}'.format(i['ReplicationGroupId'],tag))
    return clustername

def resize_redis(service,region,tags,properties):
    client = boto3.client(service, region_name=region[0])
    clustername = fetch_redis_cluster(service,properties['region'],tags)
   # new = modification['redisModification']
    for redis in clustername:
        response = client.modify_replication_group(
        ReplicationGroupId=redis,
        ApplyImmediately=True, 
        CacheNodeType=properties['services']['redis']['resize_params']['cacheNodeType']
        )  
        logging.info(f'Modification of {redis} redis done.')
def _scheduleFactory(properties, aws_profile, args):

    instance_ids = []
            
    try:
        
        LOGGER.info(f'Connecting to AWS.')

        if aws_profile:
            session = generate_aws_session._create_session(aws_profile)
        else:
            session = generate_aws_session._create_session()

        LOGGER.info(f'Connection to AWS established.')
        # services = properties['services']
        # print(services.keys())
        for property in properties['services']:
            # print(properties['services'])
            # print(property)
            if property == "ec2":
                
                LOGGER.info(f'Reading ec2 tags')
                # print(properties['services']['ec2'])
                for tag in properties['services']['ec2']:
                    if tag == "tags":
                        ec2_tags = properties['services']['ec2']['tags']
                    else:
                        ec2_tags = properties['tags']
                    # print(ec2_tags)
                
                
                if ec2_tags:

                    LOGGER.info(f'Found Ec2 tags details for filtering : {ec2_tags}')

                    
                    # ec2_client = session.client("ec2", region_name=properties['region'])
                    # aws_ec2_action = aws_ec2_actions_factory.awsEC2Actions(ec2_client)

    
                    LOGGER.info(f'Scanning AWS EC2 resources in {properties["region"]} region based on tags {ec2_tags} provided')
                    instances = fetch_instance('ec2',properties['region'],ec2_tags)
                    # print(instances)

    #                 instance_ids = _fetch_instance_ids(ec2_client, "ec2", ec2_tags)

    #                 if instance_ids:

    #                     LOGGER.info(f'Found AWS EC2 resources {instance_ids} in  {properties["region"]} region based on tags provided: {ec2_tags}',extra={"ec2_ids": instance_ids})

    #                     if os.environ[SCHEULE_ACTION_ENV_KEY] == "start":
                            
    #                         aws_ec2_action._ec2_perform_action(instance_ids,action="start")

    #                     elif os.environ[SCHEULE_ACTION_ENV_KEY] == "stop":

    #                         aws_ec2_action._ec2_perform_action(instance_ids,action="stop")

    #                     else:
    #                         logging.error(f"{SCHEULE_ACTION_ENV_KEY} env not set")

    #                 else:
    #                     LOGGER.warning(f'No Ec2 instances found on the basis of tag filters provided in conf file in region {properties["region"]} ',extra={"ec2_ids": instance_ids})
    #             else:
    #                 LOGGER.warning(f'Found ec2_tags key in config file but no Ec2 tags details mentioned for filtering',extra={"ec2_ids": instance_ids})

               

                
            elif property == "rds":

                LOGGER.info(f'Reading RDS tags')
                for tag in properties['services']['rds']:
                    if tag == "tags":
                        rds_tags = properties['services']['rds']['tags']
                    else:
                        rds_tags = properties['tags']

                if rds_tags:

                    LOGGER.info(f'Found RDS tags details for filtering : {rds_tags}')
                    # print(str(properties['region']))

                    LOGGER.info(f'Scanning AWS RDS resources in {properties["region"]} region based on tags {rds_tags} provided')
                    databases = fetch_rds_db_instance('rds',properties['region'],rds_tags)

                    if databases:

                        LOGGER.info(f'Found AWS RDS resources {databases} in  {properties["region"]} region based on tags provided: {rds_tags}',extra={"Databases": databases})

                        if os.environ[SCHEULE_ACTION_ENV_KEY] == "resize":

                            rds_db_modification('rds',properties['region'],rds_tags,properties)                            

                        else:
                            logging.error(f"{SCHEULE_ACTION_ENV_KEY} env not set")
                    
                    else:
                        LOGGER.warning(f'No RDS instances found on the basis of tag filters provided in conf file in region {properties["region"]} ',extra={"rds_names": databases})
                else:
            
                    LOGGER.warning(f'Found rds_tags key in config file but no RDS tags details mentioned for filtering',extra={"rds_names": databases})

            elif property == "redis":

                LOGGER.info(f'Reading Redis tags')
                for tag in properties['services']['redis']:
                    if tag == "tags":
                        redis_tags = properties['services']['redis']['tags']
                    else:
                        redis_tags = properties['tags']

                if redis_tags:

                    LOGGER.info(f'Found Redis tags details for filtering : {redis_tags}')
                    # print(str(properties['region']))

                    LOGGER.info(f'Scanning redis resources in {properties["region"]} region based on tags {redis_tags} provided')
                    # databases = fetch_rds_db_instance('rds',properties['region'],redis_tags)
                    clustername = fetch_redis_cluster('elasticache',properties['region'],redis_tags)

                    if clustername:

                        LOGGER.info(f'Found Redis resources {clustername} in  {properties["region"]} region based on tags provided: {redis_tags}',extra={"ClustersName": clustername})

                        if os.environ[SCHEULE_ACTION_ENV_KEY] == "resize":

                            # rds_db_modification('rds',properties['region'],redis_tags,properties)      
                            resize_redis('elasticache',properties['region'],redis_tags,properties)                      

                        else:
                            logging.error(f"{SCHEULE_ACTION_ENV_KEY} env not set")
                    
                    else:
                        LOGGER.warning(f'No Redis found on the basis of tag filters provided in conf file in region {properties["region"]} ',extra={"redis_names": clustername})
                else:
                    LOGGER.warning(f'Found redis_tags key in config file but no Redis tags details mentioned for filtering',extra={"redis_names": clustername})
                
            else:
                LOGGER.info("Scanning AWS service details in config")
                
                
            

    except ClientError as e:
        if "An error occurred (AuthFailure)" in str(e):
            raise Exception('AWS Authentication Failure!!!! .. Please mention valid AWS profile in property file or use valid IAM role ').with_traceback(e.__traceback__)    
        else:
            raise e
    except KeyError as e:
        raise Exception(f'Failed fetching env {SCHEULE_ACTION_ENV_KEY} value. Please add this env variable').with_traceback(e.__traceback__)    


def _scheduleResources(args):

    LOGGER.info(f'Fetching properties from conf file: {args.property_file_path}.')

    properties = load_yaml_config._getProperty(args.property_file_path)

    LOGGER.info(f'Properties fetched from conf file.')

    if properties:
        if "aws_profile" in properties:
            aws_profile = properties['aws_profile']
        else:
            aws_profile = None
        
        _scheduleFactory(properties, aws_profile, args)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--property-file-path", help="Provide path of property file", default = os.environ[CONF_PATH_ENV_KEY], type=str)
    args = parser.parse_args()
    _scheduleResources(args)

    # print(args)


