import boto3
import datetime
import json

class Backup:

    def __init__(self):
        # dynamodbに接続
        #self.dynamodb = boto3.resource('dynamodb')
        #self.client = boto3.client('dynamodb')
        pass

    def create_target_list(self):

        # リスト保存先のS3バケット
        bucket = 'dynamodb-test'

        # バックアップ対象のリスト名
        target_list = 'dynamodb_backup_target.list'

        # s3へ接続
        s3 = boto3.resource('s3')

        # s3にあるバックアップリストを/tmpヘコピー
        s3.Bucket(bucket).download_file(target_list, '/tmp/dynamodb_backup_target.list')

        target_list = []

        with open('/tmp/dynamodb_backup_target.list', 'rt') as f:
            for line in f:
                target_list.append(line)
                print(line)

        return target_list

    def backup_exe(self, target_list, today):

        # dynamodbに接続
        client = boto3.client('dynamodb')

        for name in target_list:
            name = name.replace('\n','')
            res = client.create_backup(
                TableName = name,
                BackupName = name + '.' + today
            )
        return res

    def lotate(self, target_list, today):

        # dynamodbに接続
        client = boto3.client('dynamodb')

        # バックアップ情報取得メソッドの引数を作成
        year = str(today[0:4])
        month = str(today[4:6])
        day = str(today[6:8])
        mod_today = year + ',' + month + ',' + day
        print(mod_today)

        for name in target_list:
            name = name.replace('\n','')
            res = client.list_backups(
                TableName = name,
                #TimeRangeLowerBound = datetime.datetime(year + ',' + month + ',' + day)
                TimeRangeLowerBound = datetime.datetime(2017, 12, 26)
            )
        print(res)
'''
        if res in 'CREATING':
            print('Backup job is still in progress...')
        else:
            response = client.delete_backup(
                BackupArn = res[]
            )
'''

def lambda_handler(event, context):

    # バックアップファイル名に付与する日付のsuffix
    now = datetime.datetime.now()
    today = now.strftime("%Y%m%d")

    print('Loading function')

    # バックアップインスタンス作成
    backup = Backup()

    # バックアップのターゲットリスト読み出し
    target_list = backup.create_target_list()

    # バックアップ実行
    backup.backup_exe(target_list, today)

    # バックアップ完了のチェック & 古いバックアップの削除
    backup.lotate(target_list, today)

