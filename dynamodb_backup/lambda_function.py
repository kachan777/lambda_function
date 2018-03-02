import boto3
import datetime
import dateutil.parser
import os
from time import sleep

class Backup:

    def __init__(self):
        # dynamodbに接続
        self.client = boto3.client('dynamodb')

        # SNSへ接続
        self.sns = boto3.client('sns')

        # 実行時間の情報を作成(バックアップファイル名に日付を付与、ファイルローテートの際の比較に使用)
        self.new_backup_day = datetime.datetime.now(dateutil.tz.tzlocal())
        self.filename_suffix = self.new_backup_day.strftime("%Y%m%d")

        # 環境変数より日次バックアップの世代数を読み込み
        self.backup_generation = int(os.environ['backup_generation'])

        # 環境変数よりバックアップ対象のテーブルを読み込み、リストを作成
        self.target_list = os.environ['tables'].split(",")


    def execute(self):
        response = ""
        # リスト"target_list"からバックアップ対象のテーブルを読み込み、バックアップを順次実行
        for name in self.target_list:
            try:
                self.client.create_backup(
                    TableName = name,
                    BackupName = name + '.' + self.filename_suffix
                )
                # CreateBackupのAPIは最大50回/秒の制限があるため、スリープを入れる
                sleep(0.02)
                response += "Backup job start [" + name + "].\n"
            except:
                response += "Backup job failed [" + name + "].\n"
        print(response)

        # responseにメールの件名にジョブの成否を入れるための文字列生成
        if 'failed' in response:
            return response, '[Backup job failed] '
        else:
            return response, '[Backup job start] '

    def rotate(self):
        response2 = ""
        # リスト"target_list"からバックアップ対象のテーブルを読み込み、各バックアップファイルごとの情報を取得し、ローテーション処理を実施
        try:
            for name in self.target_list:
                res = self.client.list_backups(
                    TableName = name
                )
                # list_backupsのAPIは最大5回/秒の制限があるため、スリープを入れる
                sleep(0.2)

                # 最新のバックアップが成功していることを確認
                if 'AVAILABLE' in res['BackupSummaries'][-1]['BackupStatus']:
                    response2 += "Backup success! [" + name + "]\n"
                    # 最新のバックアップが成功しているなら、バックアップの保持期間を比較し、削除対象であれば削除実行
                    for item in res['BackupSummaries']:
                        # 検査対象のバックアップファイルのタイムスタンプを取得
                        old_backup_day =  dateutil.parser.parse(str(item['BackupCreationDateTime']))
                        # 検査対象と最新のバックアップファイルのタイムスタンプを比較、保持期間を超えているか判断し削除実行
                        if (self.new_backup_day - old_backup_day).days > self.backup_generation - 1:
                            self.client.delete_backup(
                                BackupArn = item['BackupArn']
                            )
                            response2 += item['BackupName'] + " has been deleted.\n"
                            # DeleteBackupのAPIは最大10回/秒の制限があるため、スリープを入れる
                            sleep(0.1)
                else:
                    response2 += "Rotate job failed [" + name + "].\n"
        except:
            response2 += "Rotate job failed [" + name + "].\n"

        print(response2)

        # responseにメールの件名にジョブの成否を入れるための文字列生成
        if 'failed' in response2:
            return response2, '[Rotate job failed] '
        else:
            return response2, '[Rotate job success] '

    def notify(self, text, subject):
        self.sns.publish(
            TopicArn = os.environ['SnsTopicArn'],
            Message = text,
            Subject = subject
        )

def lambda_handler(event, context):

    print("Loading function")

    # バックアップインスタンス作成
    backup = Backup()

    # バックアップ実行(機能を有効にしたいときはアンコメントしてください)
    #text = backup.execute()

    # バックアップ完了のチェック & 古いバックアップの削除(機能を有効にしたいときはアンコメントしてください)
    #text = backup.rotate()

    # バックアップ、ローテーションの結果をメール通知
    backup.notify(text[0], text[1] + context.function_name + " notification")

