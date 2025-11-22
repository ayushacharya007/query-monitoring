#!/usr/bin/env python3
import os
import aws_cdk as cdk
from stacks.query_monitoring_stack import QueryMonitoringStack
from dotenv import load_dotenv
load_dotenv()

app = cdk.App()

QueryMonitoringStack(app, "QueryMonitoringStack", 
    description="This stack creates a Lambda function that monitors Athena queries and stores the results in S3 and Glue."
)

app.synth()
