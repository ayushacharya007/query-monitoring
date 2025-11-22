from aws_cdk import (
    Stack,
    aws_iam as iam,
    aws_lambda as _lambda,
    Duration,
    aws_events as _events,
    aws_sqs as _sqs,
    aws_events_targets as _events_targets,
    aws_lambda_event_sources as _lambda_event_sources,
    RemovalPolicy,
    aws_s3 as s3,
    aws_glue as glue,
)

import aws_cdk as cdk
from constructs import Construct
import os


class QueryMonitoringStack(Stack):
    '''CDK stack that creates a Lambda function to interact with AWS Glue.
    
    - Creates an S3 bucket to store query details.
    - Creates a Glue Database to store the query monitoring data.
    - Creates a Lambda function (with awswrangler layer) that can interact
      with AWS Glue.
    '''
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        
        # Dead Letter Queue
        dlq = _sqs.Queue(self, "QueryMonitoringDLQ",
                         queue_name="QueryMonitoringDLQ",
                         retention_period=Duration.days(14),
                         removal_policy=RemovalPolicy.DESTROY
                         )
        
        
        # SQS Queue to store Events
        query_store_queue = _sqs.Queue(self, "QueryStoreQueue",
                                       queue_name="QueryStoreQueue",
                                       dead_letter_queue=_sqs.DeadLetterQueue(
                                           max_receive_count=1,
                                           queue=dlq
                                       ),
                                       visibility_timeout=Duration.minutes(15),
                                       retention_period=Duration.days(5),
                                       removal_policy=RemovalPolicy.DESTROY
                                       )

        # S3 Bucket to store the query monitoring data
        query_details_bucket = s3.Bucket(self, "QueryDetailsBucket",
                            bucket_name=f's3-query-monitoring-{cdk.Aws.ACCOUNT_ID}-{cdk.Aws.REGION}',
                            versioned=False,
                            encryption=s3.BucketEncryption.S3_MANAGED,
                            removal_policy=RemovalPolicy.DESTROY,
                            auto_delete_objects=True,
                            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                            enforce_ssl=True
                            )
        

        # Glue Database to store the query monitoring data
        query_monitoring_database = glue.CfnDatabase(self, "QueryMonitoringDatabase",
            catalog_id=cdk.Aws.ACCOUNT_ID,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="query_monitoring_db",
                description="Database for query monitoring and quality checks.",
            )
        )

        # AWS Wrangler Layer
        wrangler_layer = _lambda.LayerVersion.from_layer_version_arn(self, "SharedAwsWranglerLayer",
            layer_version_arn=os.environ["LAMBDA_LAYER_ARN"]
        )   

        # Create the Lambda function that interacts with AWS Glue.
        query_monitoring_lambda = _lambda.Function(self, "QueryMonitoringLambda",
                                       function_name="QueryMonitoringLambda",
                                       runtime=_lambda.Runtime.PYTHON_3_13,
                                       handler="query_monitoring_lambda.handler",
                                       code=_lambda.Code.from_asset("lambdas"),
                                       timeout=Duration.minutes(15),
                                       memory_size=1024,
                                       layers=[wrangler_layer],
                                       environment={
                                           "BUCKET_NAME": query_details_bucket.bucket_name,
                                           "DATABASE_NAME": query_monitoring_database.ref,
                                           "REGION": cdk.Aws.REGION,
                                           "ACCOUNT_ID": cdk.Aws.ACCOUNT_ID,
                                           "QUEUE_URL": query_store_queue.queue_url
                                        },
                                        architecture=_lambda.Architecture.ARM_64
                                       )
        
        # Event Pattern
        athena_event_pattern = _events.EventPattern(
            source=["aws.athena"],
            detail_type=["Athena Query State Change"],
            detail={
                "currentState": ["SUCCEEDED", "FAILED", "CANCELLED"]
            }
        )   
        
        # EventBridge Rule for Athena Query Changes
        eventbridge_rule = _events.Rule(self, "AthenaQueryChangesRule",
                                        rule_name="AthenaQueryChangesRule",
                                        event_pattern=athena_event_pattern,
                                        targets=[_events_targets.SqsQueue(query_store_queue)] # type: ignore
                                        )
        
        # Grant the Lambda function permissions to read messages from the SQS queue
        query_store_queue.grant_consume_messages(query_monitoring_lambda)
        
        # Grant the Lambda function access to Glue Database and Table operations
        query_monitoring_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:GetDatabase",
                    "glue:GetTable",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                ],
                effect=iam.Effect.ALLOW,
                resources=[
                    f"arn:aws:glue:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:catalog",
                    f"arn:aws:glue:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:database/{query_monitoring_database.ref}",
                    f"arn:aws:glue:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:table/{query_monitoring_database.ref}/*",
                ]
            )
        )
        
        # Grant athena query permissions to the Lambda function
        query_monitoring_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "athena:GetQueryExecution",
                    "athena:BatchGetQueryExecution",
                ],
                resources=["*"]  # batch_get_query_execution requires wildcard
            )
        )
        
        query_monitoring_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "athena:ListQueryExecutions",
                ],
                resources=[
                    f"arn:aws:athena:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:workgroup/primary",
                ]
            )
        )
        
        # Schedule the Lambda to run every 2 hours
        _events.Rule(self, "QueryBatchProcessingRule",
            schedule=_events.Schedule.cron(minute="0", hour="*/2"),
            targets=[_events_targets.LambdaFunction(query_monitoring_lambda)] #type: ignore
        )
        
        # Grant SSM permissions for checkpoint tracking
        query_monitoring_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "ssm:GetParameter",
                    "ssm:PutParameter",
                ],
                resources=[
                    f"arn:aws:ssm:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:parameter/query-monitoring/*"
                ]
            )
        )
        
        # Grant the Lambda function read/write access to the result bucket
        query_details_bucket.grant_read_write(query_monitoring_lambda)
