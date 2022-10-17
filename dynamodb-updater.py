import boto3, sys
from datetime import datetime

dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table(sys.argv[1])

print(table.item_count)

all_items = table.scan()

timestamp = datetime.now().isoformat()

for e in all_items['Items']:
    table.update_item(
        Key = {
            'id': e['id']
        },
        UpdateExpression = 'SET lastIngestion = :ts',
        ExpressionAttributeValues = {
            ':ts': timestamp
        }
    )
