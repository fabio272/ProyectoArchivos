#!/usr/bin/python
# importa librerias necesarias para el proyecto 

import time
import sys
from datetime import datetime
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from pyspark.sql.functions import unix_timestamp, lit, array, col, from_unixtime
from pyspark.sql import functions as F

# Inicia sesion de spark - Define nombre de app y configura timeout

spark = SparkSession.builder.appName("ProyectoArchivos").config("spark.sql.broadcastTimeout", "36000").getOrCreate()

# Recibe lista de parametros. Admite un maximo de 3 en el siguiente orden: Path de archivo Config, Lista campos a añadir, lista de valores de campos a añadir respectivamente (Valor de campo nuevo admite "current_date" para fecha actual (int o date en el archivo de configuración))

args = sys.argv

#################################################################################
#               BLOQUE PRUEBAS LOCALES EN EL AMBIENTE VIRTUAL                   #
#args = ['/path/Config_nyse_2012.csv','partition_date','current_date']          #
#args = ['/path/Config_test.csv']                                               #
#################################################################################

# Evalua la cantidad de parametros que recibe y le asigna valor a sus variables correspondientes

countArgs=''
for i in range(0,len(args)):
  countArgs = 0+i
if countArgs == 1:
  configFile = args[1]
else:
  configFile, newFields, newFieldsValues = args[1] , args[2], args[3]

#################################################################################
#               BLOQUE PRUEBAS LOCALES EN EL AMBIENTE VIRTUAL                   #
#countArgs=''                                                                   #
#for i in range(0,len(args)):                                                   #
#    countArgs = 0+i                                                            #
#if countArgs == 0:                                                             #
#    configFile = args[0]                                                       #
#else:                                                                          #
#    configFile, newFields, newFieldsValues = args[0] , args[1], args[2]        #
#################################################################################

# Levanta archivo de configuracion para la ejecucion

dfConfig=spark.read.format("csv").option("header","true").option("sep","|").option("inferSchema","true").load(configFile)

# Convierte DataFrame PySpark a Pandas DF - inicializa variables de archivo Config - Asigna valor correspondiente a variables por CLAVE = VALOR

df_KeyValue = dfConfig.toPandas()
print('Variables de archivo Config:')
for index, row in df_KeyValue.iterrows():
    locals()[row['CLAVE']]=row['VALOR']
print('- Variables seteadas!')
print('--------------------------------------------------------')    

# Evalua si existe particion por defecto Current_Date y su formato

try:
  if 'defaultPartition' in locals() and typePartition == 'date':
    now = datetime.now()
    currentTimePartition = datetime.now().strftime("%Y-%m-%d")
  else:
    now = datetime.now()
    currentTimePartition = datetime.now().strftime("%Y%m%d")
  print('- Particion por defecto: '+ defaultPartition +'='+ str(currentTimePartition))
  print('--------------------------------------------------------')
except:
  print('- No hay particion por defecto')
  print('--------------------------------------------------------')
  None

# Evalua si hay campos nuevos a adherir al archivo y crea un diccionario con {Nombre: Valor}.

try:
    newFieldsArray = newFields.rsplit(sepArg)
    newFieldsValuesArray = newFieldsValues.rsplit(sepArg)
    dictNewFields = dict(zip(newFieldsArray, newFieldsValuesArray))
    print('Diccionario de nuevos campos:')
    print('- '+ str(dictNewFields))
    print('--------------------------------------------------------')
except:
    print('- No hay nuevos campos a añadir')
    print('--------------------------------------------------------')
    None

# Crea arreglo de campos DATE - TIMESTAMP- Float - Int.

try:
    dateArray = dateType.rsplit(sepVariable)
except:
    None

try:
    timestampArray = timestampType.rsplit(sepVariable)
except:
    None

try:
    floatArray = floatType.rsplit(sepVariable)
except:
    None

try:
    intArray = intType.rsplit(sepVariable)
except:
    None

# Toma el archivo a replicar desde el path de Origen.
# Archivo de tipo de ancho fijo
if fwf == 'TRUE':
  fwfLenghts = fwfLenghts.split(sepVariable)
  fwfLenghts = list(map(int, fwfLenghts))
  columnNames = columnNames.split(sepVariable)
  df_final_pd = pd.read_fwf(sourcePath, widths=fwfLenghts)
  if header == 'False':
    df_final_pd.columns = columnNames
    df_final = spark.createDataFrame(df_final_pd)
  else:
    df_final = spark.createDataFrame(df_final_pd)
  print('Levanta archivo fwf Origen')
  print('- '+ sourcePath)
  print('--------------------------------------------------------')
# Archivo delimitado por separador
else:
  df_final = spark.read.csv(sourcePath, header=header, sep=sep)
  print('Levanta archivo Origen')
  print('- '+ sourcePath)
  print('--------------------------------------------------------')

# Castea los campos de tipo Date
print('Evalua si hay campos DATE, para respectivo casteo:')
try:
    for j in range(0,len(dateArray)):
        df_final = df_final.withColumn(dateArray[j], F.to_date(col(dateArray[j]),dateFormat))
        print('- Campo "'+ dateArray[j] +'" CASTEADO a Date')
except:
    print('- No existen campos en formato Date en el DataFrame')
    None
print('--------------------------------------------------------')

# Castea los campos de tipo Timestamp
print('Evalua si hay campos TIMESTAMP, para respectivo casteo:')
try:
    for j in range(0,len(timestampArray)):
        df_final = df_final.withColumn(timestampArray[j], F.unix_timestamp(timestampArray[j], timestampFormat).cast(TimestampType()))
        print('- Campo "'+ timestampArray[j] +'" CASTEADO a Timestamp')
except:
    print('- No existen campos en formato Timestamp en el DataFrame')
    None
print('--------------------------------------------------------')

# Castea los campos de tipo Float
print('Evalua si hay campos Float para respectivo casteo:')
try:
    for j in range(0,len(floatArray)):
        df_final = df_final.withColumn(floatArray[j], col(floatArray[j]).cast(FloatType()))
        print('- Campo "'+ floatArray[j] +'" CASTEADO a Float')
except:
    print('- No existen campos en formato Float en el DataFrame')
    None
print('--------------------------------------------------------')

# Castea los campos de tipo Int
print('Evalua si hay campos Int para respectivo casteo:')
try:
    for j in range(0,len(intArray)):
        df_final = df_final.withColumn(intArray[j], col(intArray[j]).cast(IntType()))
        print('- Campo "'+ intArray[j] +'" CASTEADO a Int')
except:
    print('- No existen campos en formato Int en el DataFrame')
    None
print('--------------------------------------------------------')

# Adhiere nuevos campos en caso de existir
print('Evalua si adhiere nuevas columnas y escribe el archivo:')
try:
  for key, value in dictNewFields.items():
    if value == 'current_date':
      if typePartition == 'date':
        now = datetime.now()
        currentTimePartition = datetime.now().strftime("%Y-%m-%d")
      else:
        now = datetime.now()
        currentTimePartition = datetime.now().strftime("%Y%m%d")
      df_final = df_final.withColumn(key, lit(currentTimePartition))
      print('- Adhiere columna: '+ key)
    else:
      df_final = df_final.withColumn(key, lit(value))
      print('- Adhiere columna: '+ key)
except:
  print('- No hay columnas nuevas a añadir')
  None

#Evalua la particion, si esta seteada por defecto le asigna los valores configurados de fecha actual.
if partition == 'default':
  df_final = df_final.withColumn(defaultPartition, lit(currentTimePartition))
  partition = defaultPartition
else:
  None
# Escribe archivo
try:
  df_final.write.mode(writeMode).format(writeFormat).partitionBy(partition).save(finalPath)
  print('- Escribe archivo particionado:')
  df_final.show(10,truncate = False)

except:
  df_final.write.mode(writeMode).format(writeFormat).save(finalPath)
  print('- Escribe archivo sin particiones:')
  df_final.show(10, truncate = False)
        
print('--------------------------------------------------------')        
print('Se replico el archivo con éxito')