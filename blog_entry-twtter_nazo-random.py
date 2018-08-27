# -*- coding: utf-8 -*-

import os
import sys
import boto3
import random
from boto3.dynamodb.conditions import Key, Attr

#
# DynamoDBに格納されたTwitter謎のURLを指定数取得
# 引数 : 取得する謎の数
# 戻値 : Twitter謎のURL、TwitterアカウントのIDを含むDBデータ
#
def getNazoFromDynamo(count):

    tableName = "nazo-tweet-tables"
    tableName_seq = "sequences"
    key = "no"
    key_seq = "my_table"

    dynamodb = boto3.resource(
        'dynamodb',
        region_name=os.getenv("AWS_Resion_Name"),
        aws_access_key_id=os.getenv("AWS_Access_Key_Id"),
        aws_secret_access_key=os.getenv("AWS_Secret_Access_Key")
    )

    table = dynamodb.Table(tableName)
    table_seq = dynamodb.Table(tableName_seq)

    # これまで集計したTwitter謎の総数を取得
    try:
        maxNazoCount = table_seq.query(
             KeyConditionExpression=Key('name').eq(key_seq)
        )['Items'][0]['current_number']
    except:
        return

    #
    # 取得した謎noをDynamoDBのscan用のフィルタ（FilterExpression）に
    # OR条件の条件文を作成する
    #
    # 参考：https://stackoverflow.com/questions/48697000/python-dynamodb-construct-dynamic-queries-combined-with-boolean-operations
    #

    while True:

        #
        # Twitter謎の番号を配列に格納し、そこからランダムで指定個数を配列に格納する
        #　DynamoDBに格納した後にTwitter謎ではないと判断され削除したものがあるため、
        # 指定した数字の情報がない可能性があるのでwhileで無限ループを行う
        #
        nazoList = list(range(1,maxNazoCount,1))
        nazoListChoice = random.sample(nazoList, count)

        fe_exclusion = None
        request = {}

        for x in nazoListChoice:
            fe_x = Attr('no').eq(x)

            if not fe_exclusion:
                fe_exclusion = fe_x
            else:
                fe_exclusion = (fe_exclusion | fe_x)

        fe = fe_exclusion
        request['FilterExpression'] = fe

        try:
            response = table.scan(**request)
        except:
            return

        #
        # 取得したscan結果が引数のcount数より小さい場合はもう一度データの収集を試みる
        #
        if response['Count'] >= count:
            break
        else:
            continue

    return response['Items']

def respond(httpStatusCode, data):

    return {
        'isBase64Encoded': False,
        'statusCode': httpStatusCode,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': '' if data is None else str(data)
    }

def lambda_handler(event, context):

    # 取得する謎の数を引数から取得する
    if event['queryStringParameters'].has_key('count'):
        count = int(event['queryStringParameters']['count'])
    else:
        return respond('500', '')

    #
    # DBからデータの取得を行う
    #
    result = getNazoFromDynamo(count)

    if result <> None:
        result = '{"result": ' + str([{"id": str(d.get("id")), "tweet_id": str(d.get("tweet_id"))} for d in result]).replace("\'","\"") + '}'

        return respond('200', result)
    else:
        return respond('500', '')
