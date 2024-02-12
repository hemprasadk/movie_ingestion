import json
import urllib.parse
import boto3
import hashlib
from datetime import datetime
print('Loading function')

s3 = boto3.client('s3')


def update_dynamodb_item(table, primary_key_value, json_object,row_hash):
    current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    try:
        # If the item exists, mark it as obsolete by setting endDate
        filter_attribute = 'id'
        filter_value = primary_key_value
        sort_key_condition = 'endDate = :sort_val'
        sort_key_value = 'null'
        index_name='id-end_date-index'
        response = table.query(
            IndexName=index_name,
            KeyConditionExpression=f"{filter_attribute} = :val and end_date = :sort_val",
            ExpressionAttributeValues={
                ':val': filter_value,
                ':sort_val': sort_key_value
            },
            Select='ALL_ATTRIBUTES'  # Change this as per your requirements
        )
        for item in response['Items']:
            row_hash_old=item['row_hash']
            primary_key={'row_hash':row_hash_old}
            table.update_item(
                Key=primary_key,
                UpdateExpression="SET end_date = :end_date",
                ExpressionAttributeValues={":end_date": str(current_time)}
            )
        dynamo_item = {key: json.dumps(value) for key, value in json_object.items()}
        dynamo_item.update({
            'row_hash':str(row_hash),
            'id': primary_key_value,
            'startDate': current_time,
            'end_date': 'null'
        })
        print(dynamo_item)
        table.put_item(Item=dynamo_item)
        return True
    except Exception as e:
        print(f"Error updating DynamoDB item: {e}")
        return False


def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print("CONTENT TYPE: " + response['ContentType'])
        json_objects = json.loads(response['Body'].read())
        dynamodb_table_name = 'movie-details-table'
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(dynamodb_table_name)
        if json_objects:
            for json_object in json_objects:
                # Convert JSON object to string
                json_string = json.dumps(json_object, sort_keys=True)
                # Compute hash of the string
                hash_object = hashlib.sha256(json_string.encode())
                hash_value = hash_object.hexdigest()
                row_hash = hash_value
                primary_key_value = str(json_object['year'])+"_"+json_object['title'].replace(" ","")
                # Update DynamoDB item
                update_dynamodb_item(table, primary_key_value, json_object,row_hash)
        return response['ContentType']
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
        