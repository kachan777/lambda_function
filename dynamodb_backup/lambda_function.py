import boto3
import datetime
import json

class Backup:

    def __init__(self):
        # dynamodbに接続
        self.dynamodb = boto3.resource('dynamodb')
        self.client = boto3.client('dynamodb')

    def table_scan(self, table_name):
        table = self.dynamodb.Table(table_name)
        try:
            res = table.scan()
            return res
        except Exception as e:
            return e

    def backup_exe(self, name, today):
        print(name)
        res = self.client.create_backup(
            TableName = name,
            BackupName = name + '.' + today
        )
        return res

    def check(self):
        pass

    def lotate(self):
        pass

def lambda_handler(event, context):

    # バックアップファイル名に付与する日付のsuffix
    now = datetime.datetime.now()
    today = now.strftime("%Y%m%d")

    # バックアップ対象のリストが定義されているdynamodb table
    backup_list = 'backup_status'

    print('Loading function')

    # バックアップインスタンス作成
    backup = Backup()

    # バックアップリスト読み出し & バックアップ実行
    for keys in backup.table_scan(backup_list)['Items']:
        print(keys['name'])
        backup.backup_exe(keys['name'], today)

