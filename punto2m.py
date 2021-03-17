#Realizamos las importaciones
import json
import requests
from bs4 import BeautifulSoup
import os
import datetime as dt
import boto3
from pandas_datareader import DataReader
from datetime import timedelta
from datetime import date
from urllib.parse import unquote_plus

#Ingresamos el nombre del bucket
bucket='punto-2-parcial-big-data1'
#Ingresamos el nombre de las paginas
publimetro='publimetro'
eltiempo='eltiempo'

###Descargamos las paginas
p = requests.get('https://www.publimetro.co/')
t = requests.get('https://www.eltiempo.com')
publimetro_soup = BeautifulSoup(p.text, 'lxml')
tiempo_soup = BeautifulSoup(t.text, 'lxml')

###Creamos el archivo para Publimetro
archivo=open(publimetro+'.html','w', encoding='utf-8')
archivo.write(str(publimetro_soup))
archivo.close()
###Creamos el archivo para Publimetro
archivo1=open(eltiempo+'.html','w', encoding='utf-8')
archivo1.write(str(tiempo_soup))
archivo1.close()

#Ingresamos la fecha con dia mes y año
hoy= dt.datetime.today()

year = date.today()
agno=year.year
month=date.today()
mes=month.month
day=date.today()
dia=(day.day)


###UPLOAD El tiempo
#Será la dirección donce irá nuestro archivo
NombreArchivo = 'headlines/raw/periodico=eltiempo/agno='+str(agno)+'/mes='+str(mes)+'/dia='+str(dia)+'/eltiempo.html'
#Nombre de cómo quedará en el bucket
NombreObjeto = 'eltiempo.html'
#Llamamos al cliente de boto3
s3_client = boto3.client('s3')
response = s3_client.upload_file(NombreObjeto, bucket, NombreArchivo)
print("El archivo subió correctamente")

###UPLOAD publimetro
#Será la dirección donce irá nuestro archivo
NombreArchivo1 = 'headlines/raw/periodico=publimetro/agno='+str(agno)+'/mes='+str(mes)+'/dia='+str(dia)+'/publimetro.html'
#Nombre de cómo quedará en el bucket
NombreObjeto1 = 'publimetro.html'
#Llamamos al cliente de boto3
s3_client = boto3.client('s3')
response = s3_client.upload_file(NombreObjeto1, bucket, NombreArchivo1)
print("El archivo subió correctamente")

client =boto3.client('athena',region_name='us-east-1')
def partition(nomPeriodico):

   params = {
      'region': 'us-east-1',
      'database': 'periodico',
      'bucket': 'punto-2-parcial-big-data1',
      'path': 'temp/athena/output',
      'query': 'alter table noticias add partition(periodico="'+nomPeriodico+'",agno='+str(agno)+',mes='+str(mes)+',dia='+str(dia)+');'
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
partition('eltiempo')
partition('publimetro')
