#!/usr/bin/env python3

try:
    import configparser
except:
    from six.moves import configparser
import sys, os, argparse, logging, yaml, json
import json_log_formatter
import boto3
from botocore.exceptions import ClientError
from otawslibs import generate_aws_session , aws_resource_tag_factory , aws_ec2_actions_factory , aws_rds_actions_factory
from otfilesystemlibs import yaml_manager

SCHEULE_ACTION_ENV_KEY = "SCHEDULE_ACTION"
CONF_PATH_ENV_KEY = "CONF_PATH"
LOG_PATH = "/var/log/ot/aws-resource-scheduler.log"

FORMATTER = json_log_formatter.VerboseJSONFormatter()
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

FILE_HANDLER = logging.FileHandler(LOG_PATH)
STREAM_HANDLER = logging.StreamHandler(sys.stdout)

FILE_HANDLER.setFormatter(FORMATTER)
STREAM_HANDLER.setFormatter(FORMATTER)

LOGGER.addHandler(FILE_HANDLER)
LOGGER.addHandler(STREAM_HANDLER)
  

def resize_rds(properties,databases,client):

    logging.info(f'Start modifying the databases.....')
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


def resize_redis(properties,redis_instance_ids,client):

    for redis in redis_instance_ids:
        response = client.modify_replication_group(
        ReplicationGroupId=redis,
        ApplyImmediately=True, 
        CacheNodeType=properties['services']['redis']['resize_params']['cacheNodeType']
        )  
        logging.info(f'Modification of {redis} redis done.')

def _awsResourceManagerFactory(properties, aws_profile, args):

    instance_ids = []

    
    try:
        
        LOGGER.info(f'Connecting to AWS.')

        if aws_profile:
            session = generate_aws_session._create_session(aws_profile)
        else:
            session = generate_aws_session._create_session()

        LOGGER.info(f'Connection to AWS established.')
        
        for property in properties['services']:
             
            if property == "rds":

                LOGGER.info(f'Reading RDS tags')

                for tag in properties['services']['rds']:
                    if tag == "tags":
                        rds_tags = properties['services']['rds']['tags']
                    else:
                        rds_tags = properties['tags']

                if rds_tags:

                    LOGGER.info(f'Found RDS tags details for filtering : {rds_tags}')

                    rds_client = session.client("rds", region_name=properties['region'][0])

                    LOGGER.info(f'Scanning AWS RDS resources in {properties["region"]} region based on tags {rds_tags} provided')

                    aws_resource_finder = aws_resource_tag_factory.getResoruceFinder(rds_client,"rds")
                    databases = aws_resource_finder._get_resources_using_tags(rds_tags)

                    if databases:

                        LOGGER.info(f'Found AWS RDS resources {databases} in  {properties["region"]} region based on tags provided: {rds_tags}',extra={"Databases": databases})

                        if os.environ[SCHEULE_ACTION_ENV_KEY] == "resize":

                            resize_rds(properties,databases,rds_client)                            

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

                    redis_client = session.client("elasticache", region_name=properties['region'][0])

                    LOGGER.info(f'Scanning redis resources in {properties["region"]} region based on tags {redis_tags} provided')

                    aws_resource_finder = aws_resource_tag_factory.getResoruceFinder(redis_client,"redis")
                    redis_instance_ids = aws_resource_finder._get_resources_using_tags(redis_tags)

                    if redis_instance_ids:

                        LOGGER.info(f'Found Redis resources {redis_instance_ids} in  {properties["region"]} region based on tags provided: {redis_tags}',extra={"ClustersName": redis_instance_ids})

                        if os.environ[SCHEULE_ACTION_ENV_KEY] == "resize":

                            resize_redis(properties,redis_instance_ids,redis_client)                      

                        else:
                            logging.error(f"{SCHEULE_ACTION_ENV_KEY} env not set")
                    
                    else:
                        LOGGER.warning(f'No Redis found on the basis of tag filters provided in conf file in region {properties["region"]} ',extra={"redis_names": redis_instance_ids})
                else:
                    LOGGER.warning(f'Found redis_tags key in config file but no Redis tags details mentioned for filtering',extra={"redis_names": redis_instance_ids})
                
            else:
                LOGGER.info("Scanning AWS service details in config")
                
                

    except ClientError as e:
        if "An error occurred (AuthFailure)" in str(e):
            raise Exception('AWS Authentication Failure!!!! .. Please mention valid AWS profile in property file or use valid IAM role ').with_traceback(e.__traceback__)    
        else:
            raise e
    except KeyError as e:
        raise Exception(f'Failed fetching env {SCHEULE_ACTION_ENV_KEY} value. Please add this env variable').with_traceback(e.__traceback__)    


def _resizeResources(args):

    LOGGER.info(f'Fetching properties from conf file: {args.property_file_path}.')

    yaml_loader = yaml_manager.getYamlLoader()
    properties = yaml_loader._loadYaml(args.property_file_path)

    LOGGER.info(f'Properties fetched from conf file.')

    if properties:
        if "aws_profile" in properties:
            aws_profile = properties['aws_profile']
        else:
            aws_profile = None
        
        _awsResourceManagerFactory(properties, aws_profile, args)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--property-file-path", help="Provide path of property file", default = os.environ[CONF_PATH_ENV_KEY], type=str)
    args = parser.parse_args()
    _resizeResources(args)



