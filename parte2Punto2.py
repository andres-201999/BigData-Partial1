#Realizamos las importaciones
import json
import os
import boto3
import datetime as dt
from urllib.parse import unquote_plus
from datetime import date
from datetime import timedelta
#Iniciamos el ciente de boto3
s3 = boto3.client('s3')

def lambda_handler(event, context):
    # TODO implement
    key = unquote_plus(event['Records'][0]['s3']['object']['key'])

    nameBucket = unquote_plus(event['Records'][0]['s3']['bucket']['name'])
    #Indsicamos la direccion de descarga
    DirDescarga = '/tmp/{}'.format(key.split('/')[-1])
    #Descargamos el archivo
    s3.download_file(nameBucket,key,DirDescarga)
    #Leemos el archivo
    LeerArchivo = open(DirDescarga,'r')
    lineas = LeerArchivo.readlines()
   
    #Variables que almacenarán los datos requeridos
    CatPeriodico = ''
    CabeceraPeriodico = ''
    LinkPeriodico = ''
    #Indicamos la categoria el titulo o link
    for i in lineas:
        if 'category' in i:
            CatPeriodico = i.replace(',',' ').split('category')[1]
            break
    for i in lineas:
        if '<title>' in i:
            CabeceraPeriodico = i.replace(',',' ')
            break
    for i in lineas:
        if 'canonical' in i:
            aux = i.split(' ')
            for j in aux:
                if 'href="https://www' in j:
                    LinkPeriodico = j.split('"')[1]
            break
    #Creamos un archivo con los datos que necesitamos
    archivo = open('/tmp/prueba.txt','w')
    archivo.write('category,headline,link\n{},{},{}'.format(CatPeriodico,CabeceraPeriodico,LinkPeriodico))
    archivo.close()
    
    #asignamos variables de dia,mes,año
    hoy= dt.datetime.today()
    year = date.today()
    agno=year.year
    month=date.today()
    mes=month.month
    day=date.today()
    dia=(day.day)

    #Subimos el archivo
    upload_path='headlines/final/periodico='+key.split('/')[-1].split('.')[0]+'/agno='+str(agno)+'/mes='+str(mes)+'/dia='+str(dia)+'/respuesta.csv'
    s3.upload_file('/tmp/prueba.txt',nameBucket,upload_path)
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!!!!')
    }
    

