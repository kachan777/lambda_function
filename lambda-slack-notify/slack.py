# -*- coding: utf-8 -*-
import slackweb
import urllib

SLACK_POST_URL = "https://hooks.slack.com/services/XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

def lambda_handler(event, context):
    print(event)
    test_user = 'kachan'
    icon_emoji = ':popuko2:'
    slack_massage = urllib.unquote(event['message']).encode('raw_unicode_escape').decode('utf-8')
    slack_post(test_user, icon_emoji, slack_message)
    print(slack_message)

def slack_post(user, icon, message):
    slack = slackweb.Slack(url=SLACK_POST_URL)
    slack.notify(text=message, username=user, icon_emoji=icon)
