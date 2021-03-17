import json
import boto3
from urllib.parse import unquote_plus
from datetime import timedelta
from datetime import date
import datetime as dt
import os

s3 = boto3.client('s3')
def lambda_handler(event, context):
    # TODO implement

    hoy= dt.datetime.today()
    year = date.today()
    agno=year.year
    month=date.today()
    mes=month.month
    day=date.today()
    dia=(day.day)-1

    key = unquote_plus(event['Records'][0]['s3']['object']['key'])
    nameBucket = unquote_plus(event['Records'][0]['s3']['bucket']['name'])
    DirDescarga = '/tmp/{}'.format(key.split('/')[-1])

    newkey = 'headlines/raw/periodico='+key.split('/')[-1].split('.')[0]+'/agno='+str(agno)+'/mes='+str(mes)+'/dia='+str(dia)+'/'+key.split('/')[-1].split('.')[0]+'.html'
    print(newkey)


    s3.download_file(nameBucket,newkey,DirDescarga)

    ArchivoSubido = 'news/raw/periodico='+key.split('/')[-1].split('.')[0]+'/agno='+str(agno)+'/mes='+str(mes)+'/dia='+str(dia)+'/'+key.split('/')[-1].split('.')[0]+'.html'


    s3.upload_file(DirDescarga,nameBucket,ArchivoSubido)

    return {
        'statusCode': 200,
        'body': json.dumps(' El archivo ha subido!')
    }