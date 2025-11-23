import boto3
import awswrangler as wr
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


def process_query_ids(query_ids):
    """
    Fetches details for query IDs and saves to S3.
    """
    if not query_ids:
        return 0
        
    all_query_details = []
    logger.info(f"Starting to fetch query details for {len(query_ids)} query IDs")
    
    for i in range(0, len(query_ids), BATCH_SIZE):
        batch = query_ids[i:i + BATCH_SIZE]
        batch_details = get_query_details(batch)
        all_query_details.extend(batch_details)
    
    if all_query_details:
        logger.info(f"Successfully retrieved {len(all_query_details)} query details")
        df = pd.DataFrame(all_query_details)
        
        wr.s3.to_parquet(
            df=df,
            path=f"s3://{BUCKET_NAME}/query_details",
            index=False,
            dataset=True,
            mode="append",
            database=DATABASE_NAME,
            table="query_details",
        )
        logger.info(f"Processed {len(all_query_details)} queries.")
        
    return len(all_query_details)

def poll_sqs_and_process(context):
    """
    Polls SQS for messages, accumulates them into batches of BATCH_SIZE (50),
    and processes them. Respects Lambda timeout.
    """
    sqs = boto3.client('sqs')
    queue_url = os.getenv('QUEUE_URL')
    
    if not queue_url:
        logger.error("QUEUE_URL environment variable not set")
        return 0
        
    total_processed = 0
    accumulated_query_ids = []
    accumulated_receipt_handles = []
    
    while True:
        # Check for timeout (leave 1 minute buffer)
        if context.get_remaining_time_in_millis() < 60000:
            logger.warning("Lambda is approaching timeout. Stopping polling to flush buffer.")
            break

        # Receive messages
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10, # SQS limit
            WaitTimeSeconds=20 # Long polling
        )
        
        logger.info("Polling SQS for messages...")
        
        messages = response.get('Messages', [])
        if not messages:
            logger.info("No more messages in queue.")
            break
            
        for msg in messages:
            try:
                body = json.loads(msg['Body'])
                query_id = body.get('detail', {}).get('queryExecutionId')
                if query_id:
                    accumulated_query_ids.append(query_id)
                    accumulated_receipt_handles.append({
                        'Id': msg['MessageId'],
                        'ReceiptHandle': msg['ReceiptHandle']
                    })
            except Exception as e:
                logger.error(f"Error parsing message: {e}")
        
        # Check if we have enough messages to process a batch
        while len(accumulated_query_ids) >= BATCH_SIZE:
            # Extract a batch
            batch_ids = accumulated_query_ids[:BATCH_SIZE]
            batch_handles = accumulated_receipt_handles[:BATCH_SIZE]
            
            # Remove from accumulators
            accumulated_query_ids = accumulated_query_ids[BATCH_SIZE:]
            accumulated_receipt_handles = accumulated_receipt_handles[BATCH_SIZE:]
            
            # Process batch
            count = process_query_ids(batch_ids)
            total_processed += count
            
            # Delete processed messages in chunks of 10 (SQS limit)
            for i in range(0, len(batch_handles), 10):
                chunk = batch_handles[i:i+10]
                try:
                    sqs.delete_message_batch(QueueUrl=queue_url, Entries=chunk)
                except Exception as e:
                    logger.error(f"Failed to delete messages: {e}")

    # Process any remaining messages
    if accumulated_query_ids:
        count = process_query_ids(accumulated_query_ids)
        total_processed += count
        
        # Delete remaining messages in chunks of 10
        for i in range(0, len(accumulated_receipt_handles), 10):
            chunk = accumulated_receipt_handles[i:i+10]
            try:
                sqs.delete_message_batch(QueueUrl=queue_url, Entries=chunk)
            except Exception as e:
                logger.error(f"Failed to delete messages: {e}")
                    
    return total_processed

def handler(event, context):
    """
    Lambda handler with three modes:
    1. Initial backfill: Fetch all historical queries on first run
    2. Event-driven (SQS Trigger): Process SQS messages directly
    3. Scheduled (Polling): Poll SQS queue for messages
    """
    try:
        logger.info("Lambda handler started")
        
        # Initialize SSM client
        ssm_client = boto3.client('ssm')
        parameter_name = '/query-monitoring/initial-load-complete'
        
        # Check initial load status
        try:
            response = ssm_client.get_parameter(Name=parameter_name)
            initial_load_complete = response['Parameter']['Value'] == 'true'
            logger.info("Initial load already completed.")
        except ssm_client.exceptions.ParameterNotFound:
            initial_load_complete = False
            logger.info("Initial load not yet completed. Starting backfill...")
        
        # MODE 1: Initial Backfill
        if not initial_load_complete:
            logger.info("Running initial backfill...")
            ids = get_all_query_ids()
            
            count = process_query_ids(ids)
            
            ssm_client.put_parameter(
                Name=parameter_name,
                Value='true',
                Type='String',
                Overwrite=True,
                Description='Tracks whether initial query monitoring backfill is complete'
            )
            
            return {'statusCode': 200, 'body': json.dumps({'message': 'Backfill completed', 'count': count})}

        # MODE 2: Scheduled Event (Poll SQS)
        else:
            logger.info("Processing Scheduled Event (Polling SQS)...")
            count = poll_sqs_and_process(context)
            return {'statusCode': 200, 'body': json.dumps({'message': 'Scheduled polling completed', 'count': count})}

    except Exception as e:
        logger.error(f"Handler execution failed: {e}")
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}