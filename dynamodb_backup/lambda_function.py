import boto3
import datetime

def lambda_handler(event, context):

    # dynamodbに接続
    dynamodb = boto3.client('dynamodb')

    # table listを取得
    table_list = dynamodb.list_tables()['TableNames']

    # バックアップファイル名に付与する日付のsuffix

    now = datetime.datetime.now()
    today = now.strftime("%Y%m%d")

    for table in table_list:
        response = dynamodb.create_backup(
            TableName = table,
            BackupName = table + '.' + today
        )
