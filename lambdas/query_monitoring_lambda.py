import boto3
import os
import logging
import json
import pandas as pd
from botocore.exceptions import ClientError, BotoCoreError

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Initialize Athena client outside handler for reuse
try:
    client = boto3.client('athena')
except Exception as e:
    logger.error(f"Failed to initialize Athena client: {e}")
    raise

BUCKET_NAME = os.getenv('BUCKET_NAME')
DATABASE_NAME = os.getenv('DATABASE_NAME')
BATCH_SIZE = 50 # MAX LIMIT

def get_all_query_ids(workgroup='primary'):
    """
    Retrieves all query execution IDs from the specified Athena workgroup using pagination.
    """
    query_ids = []
    try:
        logger.info(f"Starting to fetch query IDs from workgroup: {workgroup}")
        logger.info(f"Configuration - Bucket: {BUCKET_NAME}, Database: {DATABASE_NAME}")

        response = client.list_query_executions(WorkGroup=workgroup)
        
        next_token = response.get('NextToken')
        query_ids.extend(response['QueryExecutionIds'])

        while next_token:
            response = client.list_query_executions(WorkGroup=workgroup, NextToken=next_token)
            next_token = response.get('NextToken')
            query_ids.extend(response['QueryExecutionIds'])

        logger.info(f"Successfully retrieved total {len(query_ids)} query IDs")

        return query_ids
    except ClientError as e:
        logger.error(f"AWS ClientError in get_all_query_ids: {e}")
        raise
    except BotoCoreError as e:
        logger.error(f"AWS BotoCoreError in get_all_query_ids: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in get_all_query_ids: {e}")
        raise
    
    
def get_query_details(query_id: list):
    """
    Retrieves details of a specific query execution using its ID.
    """
    try:
        logger.info(f"Starting to fetch query details for {len(query_id)} query IDs")
        response = client.batch_get_query_execution(QueryExecutionIds=query_id)
        
        # Create a list to store the extracted details
        extracted_list = []
        
        # Extract the list of executions from the response
        executions = response.get("QueryExecutions", [])
        if not executions and "QueryExecutionId" in response:
             executions = [response]

        # Loop through the list of executions and extract the details
        for query in executions:
            extracted = {
                "ID": query.get("QueryExecutionId"),
                "Query": query.get("Query"),
                "State": query.get("Status", {}).get("State"),
                "Date": str(query.get("Status", {}).get("SubmissionDateTime")),
                "RunTime": f"{query.get('Statistics', {}).get('TotalExecutionTimeInMillis', 0) / 1000} sec",
                "DataScanned": f"{query.get('Statistics', {}).get('DataScannedInBytes', 0)} bytes",
                "WorkGroup": query.get("WorkGroup"),
                "QueryType": query.get("StatementType"),
                "QueryOutputLocation": query.get("ResultConfiguration", {}).get("OutputLocation"),
            }
            extracted_list.append(extracted)
            
        logger.info(f"Successfully retrieved {len(extracted_list)} queries details")
        return extracted_list

    except ClientError as e:
        logger.error(f"AWS ClientError in get_query_details: {e}")
        raise
    except BotoCoreError as e:
        logger.error(f"AWS BotoCoreError in get_query_details: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in get_query_details: {e}")
        raise
    
def handler(event, context):
    """
    Lambda handler with two modes:
    1. Initial backfill: Fetch all historical queries on first run
    2. Event-driven: Process SQS messages containing Athena query events
    """
    try:
        
        logger.info("Lambda handler started")
        
        logger.info(f"Event received: {json.dumps(event)}")
        
        # Initialize SSM client to check if initial load is complete
        ssm_client = boto3.client('ssm')
        
        parameter_name = '/query-monitoring/initial-load-complete'
        
        # Check if initial load has been completed
        try:
            response = ssm_client.get_parameter(Name=parameter_name)
            initial_load_complete = response['Parameter']['Value'] == 'true'
        except ssm_client.exceptions.ParameterNotFound:
            initial_load_complete = False
            logger.info("Initial load not yet completed. Starting backfill...")
        
        # MODE 1: Initial Backfill
        if not initial_load_complete:
            logger.info("Running initial backfill of all query executions...")
            ids = get_all_query_ids()
            all_query_details = []
            
            for i in range(0, len(ids), BATCH_SIZE):
                batch = ids[i:i + BATCH_SIZE]
                batch_details = get_query_details(batch)
                all_query_details.extend(batch_details)
                
            # Process all details into a DataFrame
            if all_query_details:
                df = pd.DataFrame(all_query_details)
                df["last_updated"] = pd.Timestamp.now()
                
                # Save to S3 using awswrangler
                import awswrangler as wr
                wr.s3.to_parquet(
                    df=df,
                    path=f"s3://{BUCKET_NAME}/data-schema/query_monitoring",
                    index=False,
                    dataset=True,
                    mode="append",
                    database=DATABASE_NAME,
                    table="query_monitoring",
                )
                
                logger.info(f"Initial backfill complete. Processed {len(all_query_details)} queries.")
                
                # Mark initial load as complete
                ssm_client.put_parameter(
                    Name=parameter_name,
                    Value='true',
                    Type='String',
                    Overwrite=True,
                    Description='Tracks whether initial query monitoring backfill is complete'
                )
            else:
                logger.info("No query details found during initial backfill.")
                
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Initial backfill completed',
                    'queries_processed': len(all_query_details)
                })
            }
        
        # MODE 2: Event-Driven Processing (SQS Messages)
        else:
            logger.info("Processing SQS messages from EventBridge...")
            
            # Extract query IDs from SQS messages
            query_ids = []
            for record in event.get('Records', []):
                try:
                    # Parse the SQS message body (which contains the EventBridge event)
                    message_body = json.loads(record['body'])
                    query_id = message_body.get('detail', {}).get('queryExecutionId')
                    
                    if query_id:
                        query_ids.append(query_id)
                        logger.info(f"Extracted query ID: {query_id}")
                except Exception as e:
                    logger.error(f"Error parsing SQS message: {e}")
                    continue
            
            if not query_ids:
                logger.info("No valid query IDs found in SQS messages.")
                return {
                    'statusCode': 200,
                    'body': json.dumps({'message': 'No queries to process'})
                }
            
            # Fetch details for the queries
            all_query_details = []
            for i in range(0, len(query_ids), BATCH_SIZE):
                batch = query_ids[i:i + BATCH_SIZE]
                batch_details = get_query_details(batch)
                all_query_details.extend(batch_details)
            
            # Save to S3
            if all_query_details:
                df = pd.DataFrame(all_query_details)
                df["last_updated"] = pd.Timestamp.now()
                
                import awswrangler as wr
                wr.s3.to_parquet(
                    df=df,
                    path=f"s3://{BUCKET_NAME}/query_details",
                    index=False,
                    dataset=True,
                    mode="append",
                    database=DATABASE_NAME,
                    table="query_details",
                )
                
                logger.info(f"Processed {len(all_query_details)} new queries from SQS.")
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Event-driven processing completed',
                    'queries_processed': len(all_query_details)
                })
            }

    except Exception as e:
        logger.error(f"Handler execution failed: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }