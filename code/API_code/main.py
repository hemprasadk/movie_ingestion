from typing import Union

from fastapi import FastAPI

app = FastAPI()
import json
import urllib.parse
import boto3
from botocore.config import Config
from boto3.dynamodb.conditions import Key, Attr
@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/movies/")
# def read_item(item_id: int, q: Union[str, None] = None):
async def read_user_item(
        year: Union[str, None] = None, title: Union[str, None] = None, cast: Union[str, None] = None, genre: Union[str, None] = None,id: Union[str, None] = None
):
    item = {"year": year, "title": title, "cast": cast, "genre": genre,'id':id}

    dynamodb_table_name = 'movie-details-table'
    my_config = Config(
        region_name='us-east-2',
        connect_timeout = 1.0,
        read_timeout = 1.0
    )
    dynamodb = boto3.resource('dynamodb', config=my_config)
    table = dynamodb.Table(dynamodb_table_name)
    # If the item exists, mark it as obsolete by setting endDate
    if id :
        filter_attribute = 'id'
        filter_value = id
        sort_key_value = 'null'
        index_name='end_date-index'
        response = table.query(
            IndexName=index_name,
            KeyConditionExpression="end_date = :sort_val",
            FilterExpression=Attr(filter_attribute).eq(filter_value),
            ExpressionAttributeValues={
                ':sort_val': sort_key_value
            },
            Select='ALL_ATTRIBUTES'  # Change this as per your requirements
        )
    elif year:
        filter_attribute = 'year'
        filter_value = year
        sort_key_value = 'null'
        index_name='end_date-index'
        response = table.query(
            IndexName=index_name,
            KeyConditionExpression="end_date = :sort_val",
            FilterExpression=Attr(filter_attribute).eq(filter_value),
            ExpressionAttributeValues={
                ':sort_val': sort_key_value
            },
            Select='ALL_ATTRIBUTES' ) # Change this as per your requirements
    elif title:
        filter_attribute = 'title'
        filter_value = title
        sort_key_value = 'null'
        index_name='end_date-index'
        response = table.query(
            IndexName=index_name,
            KeyConditionExpression="end_date = :sort_val",
            FilterExpression=Attr(filter_attribute).eq(filter_value),
            ExpressionAttributeValues={
                ':sort_val': sort_key_value
            },
            Select='ALL_ATTRIBUTES' ) # Change this as per your requirements

    elif genre:
        filter_attribute = 'genres'
        filter_value = genre
        print(genre)
        sort_key_value = 'null'
        index_name='end_date-index'
        response = table.query(
            IndexName=index_name,
            KeyConditionExpression="end_date = :sort_val",
            FilterExpression=f"contains({filter_attribute}, :val)",

            ExpressionAttributeValues={
                ':val': filter_value,
                ':sort_val': sort_key_value
            },
            Select='ALL_ATTRIBUTES' ) # Change this as per your requirements
    elif cast:
        filter_attribute = 'cast'
        filter_value = cast
        sort_key_value = 'null'
        index_name='end_date-index'
        response = table.query(
            IndexName=index_name,
            KeyConditionExpression="end_date = :sort_val",
            FilterExpression=Attr(filter_attribute).contains(filter_value),
            ExpressionAttributeValues={
                ':sort_val': sort_key_value
            },
            Select='ALL_ATTRIBUTES' ) # Change this as per your requirements
    # TODO implement

    return {
        'statusCode': 200,
        'body': response
    }
    # return item
#     dynamodb_table_name = 'movie-details-table'
#     dynamodb = boto3.resource('dynamodb')
#     table = dynamodb.Table(dynamodb_table_name)
#     # If the item exists, mark it as obsolete by setting endDate
#     if   'id'  in  event.keys():
#         filter_attribute = 'id'
#         filter_value = event['id']
#         sort_key_condition = 'endDate = :sort_val'
#         sort_key_value = 'null'
#         index_name='id-end_date-index'
#         response = table.query(
#             IndexName=index_name,
#             KeyConditionExpression=f"{filter_attribute} = :val and end_date = :sort_val",
#             ExpressionAttributeValues={
#                 ':val': filter_value,
#                 ':sort_val': sort_key_value
#             },
#             Select='ALL_ATTRIBUTES'  # Change this as per your requirements
#         )
#
# return {"item_id": item_id, "q": q}