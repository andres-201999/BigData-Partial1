import json
import pandas as pd 
import numpy as np 
import datetime as dt
import csv
from pandas_datareader import DataReader
from datetime import timedelta
from datetime import date
import boto3 
import time 

hoy= dt.datetime.today()
res= hoy  - timedelta(days=2)
Avianca = 'AVHOQ'
CementosArgos = 'CMTOY'
Ecopetrol = 'EC'
GrupoAval = 'AVAL'

AccionesAvianca = DataReader(Avianca,'yahoo',start=res)
AccionesCementosArgos = DataReader(CementosArgos,'yahoo',start=res)
AccionesEcopetrol = DataReader(Ecopetrol,'yahoo',start=res)
AccionesGrupoAval = DataReader(GrupoAval,'yahoo',start=res)

AccionesAvianca.to_csv('AccionesAvianca.csv')
AccionesCementosArgos.to_csv('AccionesCementosArgos.csv')
AccionesEcopetrol.to_csv('AccionesEcopetrol.csv')
AccionesGrupoAval.to_csv('AccionesGrupoAval.csv')

bucket='punto-1parcial-big-data'
year = date.today()
agno=year.year
print(agno)

month=date.today()
mes=month.month
print(mes)

day=date.today()
dia=day.day
print(dia)
NombreArchivo = 'Stocks/empresa=avianca/agno='+str(agno)+'/mes='+str(mes)+'/dia='+str(dia)+'/AccionesAvianca.csv'
NombreObjeto = 'AccionesAvianca.csv'

s3_client = boto3.client('s3')
response = s3_client.upload_file(NombreObjeto, bucket, NombreArchivo)
print("El archivo subió correctamente")


NombreArchivo1 = 'Stocks/empresa=CementosArgos/agno='+str(agno)+'/mes='+str(mes)+'/dia='+str(dia)+'/AccionesCementosArgos.csv'

NombreObjeto1 = 'AccionesCementosArgos.csv'

s3_client = boto3.client('s3')
response = s3_client.upload_file(NombreObjeto1, bucket, NombreArchivo1)
print("El archivo subió correctamente")

NombreArchivo2 = 'Stocks/empresa=Ecopetrol/agno='+str(agno)+'/mes='+str(mes)+'/dia='+str(dia)+'/AccionesEcopetrol.csv'

NombreObjeto2 = 'AccionesEcopetrol.csv'
response = s3_client.upload_file(NombreObjeto2, bucket, NombreArchivo2)
print("El archivo subió correctamente")

NombreArchivo3 = 'Stocks/empresa=GrupoAval/agno='+str(agno)+'/mes='+str(mes)+'/dia='+str(dia)+'/AccionesGrupoAval.csv'
NombreObjeto3 = 'AccionesGrupoAval.csv'
response = s3_client.upload_file(NombreObjeto3, bucket, NombreArchivo3)
print("El archivo subió correctamente")

client =boto3.client('athena',region_name='us-east-1')


def empresas(nomEmpresa):

   params = {
      'region': 'us-east-1',
      'database': 'parcial1',
      'bucket': 'punto-1-parcial-big-data',
      'path': 'temp/athena/output',
      'query': 'alter table acciones add partition(empresa="'+nomEmpresa+'",agno='+str(agno)+',mes='+str(mes)+',dia='+str(dia)+');'
   }
   response_query_execution_id = client.start_query_execution(
   QueryString = params['query'],
   QueryExecutionContext ={
      'Database' : params['database']
   },
   ResultConfiguration ={
      'OutputLocation': 's3://' + params['bucket']+ '/' + params['path']
   }
   ) 
empresas('Ecopetrol')
empresas('avianca')
empresas('GrupoAval')
empresas('CementosArgos')
