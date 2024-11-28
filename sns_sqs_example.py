import json

import boto3

sns = boto3.client('sns')

# Create an SNS topic
response = sns.create_topic(Name='MyTopic')
topic_arn = response['TopicArn']
print(f"Topic ARN: {topic_arn}")

sqs = boto3.client('sqs')

# Create SQS queue
response = sqs.create_queue(QueueName='MyQueue')
queue_url = response['QueueUrl']
print(f"Queue URL: {queue_url}")

# Get the Queue ARN
queue_attributes = sqs.get_queue_attributes(
    QueueUrl=queue_url,
    AttributeNames=['QueueArn']
)
queue_arn = queue_attributes['Attributes']['QueueArn']
print(f"Queue ARN: {queue_arn}")

response = sns.subscribe(
    TopicArn=topic_arn,
    Protocol='sqs',
    Endpoint=queue_arn  # SQS queue ARN
)
subscription_arn = response['SubscriptionArn']
print(f"Subscription ARN: {subscription_arn}")

# Allow the SNS topic to send messages to the SQS queue
policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": "*",
            "Action": "SQS:SendMessage",
            "Resource": queue_arn,
            "Condition": {
                "ArnEquals": {"aws:SourceArn": topic_arn}
            }
        }
    ]
}

# Attach policy to SQS queue
sqs.set_queue_attributes(
    QueueUrl=queue_url,
    Attributes={
        'Policy': json.dumps(policy)
    }
)

# Publish a message to the topic
response = sns.publish(
    TopicArn=topic_arn,
    Message='Hello, this is a test message from SNS!',
    Subject='Test Subject'
)
print(f"Message ID: {response['MessageId']}")

# Receive messages from the SQS queue
response = sqs.receive_message(
    QueueUrl=queue_url,
    MaxNumberOfMessages=10,  # Number of messages to fetch (max 10)
    WaitTimeSeconds=10       # Long polling time
)

messages = response.get('Messages', [])
for message in messages:
    print(f"Message: {message['Body']}")

    # Delete the message after processing
    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=message['ReceiptHandle']
    )

