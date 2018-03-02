#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
# lambda-instance-operation
This repository is AWS Lambda function's upload files.
"""

__authour__ = "Takeshi.Katsumata"
__version__ = 1.0

import boto3

def instance_stop(event, context):
    """ Create Connection """
    try:
        ec2 = boto3.resource('ec2', region_name='ap-northeast-1')
    except:
        print('Connection Error')
        return 1

    """ Get EC2 Instance Info """
    instance = [i for r in ec2.describe_instances() for i in r.instances]

    """ Stop EC2 Instance """
    try:
        instance.stop()
    except: 
        print('Stop EC2 Instance Error')
        return 1

    return 0
