# -----------------------------------------------------------------------------
# 
# This file is the copyrighted property of Tableau Software and is protected 
# by registered patents and other applicable U.S. and international laws and 
# regulations.
# 
# Unlicensed use of the contents of this file is prohibited. Please refer to 
# the NOTICES.txt file for further details.
# 
# -----------------------------------------------------------------------------
import sys
import csv
import time
import datetime
import locale
import array
import re
import json

import boto3
from io import BytesIO
import gzip
import os
import shutil

# Import Tableau module
from tableausdk import *
#from tableausdk.Extract import *
from tableausdk.HyperExtract import *

logs = []

# Define type maps
schemaIniTypeMap = { 
    'Bit' :     Type.BOOLEAN,
    'Byte':     Type.INTEGER,
    'Short':    Type.INTEGER,
    'Long':     Type.INTEGER,
    'Integer':  Type.INTEGER,
    'Single':   Type.DOUBLE,
    'Text':     Type.UNICODE_STRING,
    'Memo':     Type.UNICODE_STRING,
    'tinyint':  Type.INTEGER,
    'smallint': Type.INTEGER,
    'int':      Type.INTEGER,
    'int16':    Type.INTEGER,
    'int32':    Type.INTEGER,
    'int64':    Type.INTEGER,
    'bigint':   Type.INTEGER,
    'float':    Type.DOUBLE,
    'double':   Type.DOUBLE,
    'decimal':  Type.DOUBLE,
    'boolean':  Type.BOOLEAN,
    'string':   Type.UNICODE_STRING,
    'date':     Type.DATE,
    'datetime': Type.DATETIME,
    'array':    Type.UNICODE_STRING,
    'map':      Type.UNICODE_STRING,
    'object':   Type.UNICODE_STRING
}

#def parseArguments():
#    parser = argparse.ArgumentParser(description='Hyper Extract')
#    parser.add_argument('-s', '--schema', required=True)
#    parser.add_argument('-c', '--csv', required=True)
#    parser.add_argument('-o', '--output', required=True) #diretorio onde o arquivo sera salvo
#    parser.add_argument('-d', '--datasource', required=True) #nome do datasource (nome da view)
#    
#    return vars(parser.parse_args())

def getJsonSchema(s3, bucket_name, object_key):
    try:
        obj = s3.Object(bucket_name, object_key)
        content = obj.get()['Body'].read().decode('utf-8')
        schema = json.loads(content)
        return schema["schema"]
    except Exception as e:
        print(e)
        logMessage('[Error] Error on getJsonSchema: {}'.format(e))
        writeLogFile(options['logs'])

def setDate(row, colNo, value):
    if (value.find(":") != -1):
        value = value.split(' ')[0]
    d = datetime.datetime.strptime(value, "%Y-%m-%d")
    row.setDate(colNo, d.year, d.month, d.day )

def setDateTime(row, colNo, value):
    if (value.find(".") != -1):
        d = datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
    else :
        if (value.find(":") == -1):
            value = value + ' 00:00:00'
        d = datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    row.setDateTime(int(colNo), int(d.year), int(d.month), int(d.day), int(d.hour), int(d.minute), int(d.second), int(d.microsecond/100) )

def logMessage(message):
    current_datetime = datetime.datetime.now()
    current_datetime_string = current_datetime.strftime("%d/%m/%Y %H:%M:%S")
    global logs
    logs.append([current_datetime_string, message])

def writeLogFile(log_path_s3):
    global logs
    log_path = '/opt/logs.csv'
    log_s3_path = ''
    with open(log_path, 'w') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerows(logs)
    csvFile.close()
    log_filepath_s3 = log_path_s3 + '/' + 'logs.csv'
    s3_client.upload_file(log_path, bucket, log_filepath_s3)
    


fieldSetterMap = {
  Type.BOOLEAN:        lambda row, colNo, value: row.setBoolean( colNo, value.lower() == "true" ),
  Type.INTEGER:        lambda row, colNo, value: row.setInteger( colNo, int(value) ),
  Type.DOUBLE:         lambda row, colNo, value: row.setDouble( colNo, float(value) ),
  Type.UNICODE_STRING: lambda row, colNo, value: row.setString( colNo, str(value) ),
  Type.CHAR_STRING:    lambda row, colNo, value: row.setCharString( colNo, value ),
  Type.DATE:           lambda row, colNo, value: setDate(row, colNo, value),
  Type.DATETIME:       lambda row, colNo, value: setDateTime( row, colNo, value )
}

# ARGS
# Parse Arguments
options = {}
options['schema'] = os.environ['schema'] 
options['csv'] = os.environ['csv'] 
options['output'] = os.environ['output'] 
options['datasource'] = os.environ['datasource']
options['logs'] = os.environ['logs'] 
#options = parseArguments()
hyper_filename = options['output'] + '/' + options['datasource'] + '.hyper'

if not os.path.exists('/opt/output/{}/'.format(hyper_filename)):
    os.makedirs('/opt/output/{}/'.format(hyper_filename))

AWS_PUBLIC_KEY = {PUBLIC_KEY}
AWS_SECRET_KEY = {SECRET_KEY}

region = '{region}'
bucket='{Bucket}' # put your s3 bucket name here
bucket_path = 'https://s3-{}.amazonaws.com/{}'.format(region,bucket)

s3 = boto3.resource('s3', aws_access_key_id=AWS_PUBLIC_KEY,
                            aws_secret_access_key=AWS_SECRET_KEY,
                            region_name=region)
                            
s3_client = boto3.client('s3', aws_access_key_id=AWS_PUBLIC_KEY, aws_secret_access_key=AWS_SECRET_KEY)
bucket_obj = s3.Bucket(bucket)

bucket_file_object_list = []

prefix = options['csv'] + '/' + '0'

for object in bucket_obj.objects.filter(Prefix=prefix):
    bucket_file_object_list.append(object.key)

# Build Hyper Path
hyperfile = '/opt/output/' + options["datasource"] + '.hyper'


# SCHEMA
# Get CSV Schema
#schema = getJsonSchema(options['schema'])
# Read schema from s3
schema = getJsonSchema(s3, bucket, options['schema'])
hasHeader = False
colNames = []
colTypes = []

for column in schema:
    colNames.append(column['name'])
    colTypes.append(schemaIniTypeMap[column['type']])

## Open CSV file
#csvReader = csv.reader(open(csvFile, 'rt', encoding='utf8'), delimiter=';', quotechar='"', doublequote = False)

# Create HYPER output
print("Creating extract:", hyperfile)
logMessage('[Info] Creating extract: {}'.format(hyperfile))
with Extract(hyperfile) as extract:
    table = None  # set by createTable
    tableDef = None

    # Define createTable function
    def createTable(line):
        if line: 
            # append with empty columns so we have the same number of columns as the header row
            while len(colNames) < len(line): 
                colNames.append(None) 
                colTypes.append(Type.UNICODE_STRING)
            # write in the column names from the header row
            colNo = 0
            for colName in line:
                colNames[colNo] = colName
                colNo += 1
    
        # for any unnamed column, provide a default
        for i in range(0, len(colNames)):
            if colNames[i] is None:
                colNames[i] = 'F' + str(i + 1) 
        # create the schema and the table from it
        tableDef = TableDefinition()
        for i in range(0, len(colNames)):
            tableDef.addColumn( colNames[i], colTypes[i])
        table = extract.addTable("Extract", tableDef)
        return table, tableDef
    
    # Read the table
    rowNo = 0
    numFiles = len(bucket_file_object_list)
    for index, object in enumerate(bucket_file_object_list):
        file_name = object.split('/')[-1]
        print("File: {} - {} of {}".format(file_name, index+1, numFiles))
        file_name_csv = file_name.split('.')[0]
        file_name_csv = file_name_csv + '.csv'
        file_path = '/opt/' + file_name
        file_path_csv = '/opt/' + file_name_csv
        s3_client.download_file(bucket, object, file_path)
        with gzip.open(file_path, 'rb') as f_in:
            with open(file_path_csv, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(file_path)
        # Open CSV file
        csvReader = csv.reader(open(file_path_csv, 'rt', encoding='utf8'), delimiter=';', quotechar='"', doublequote = False)
        for line in csvReader:
            # Create the table upon first row (which may be a header)
            if table is None:
                table, tableDef = createTable(line if hasHeader else None)
                if hasHeader:
                    continue
        
            # We have a table, now write a row of values
            row = Row(tableDef)
            colNo = 0
            for field in line:
                if (field == ""):
                    row.setNull(colNo)
                else:
                    try :
                        fieldSetterMap[colTypes[colNo]](row, colNo, field);
                    except Exception as e:
                        print(line)
                        print(e)
                        logMessage('[Error] Error converting file. Line: {}'.format(line))
                        logMessage('[Error] Error converting file: {}'.format(e))
                        writeLogFile(options['logs'])
                        exit()
                colNo += 1
            table.insert(row)
        
            # Output progress line
            rowNo += 1
            if rowNo % 100000 == 0:
                print("{} rows inserted".format(rowNo))
        os.remove(file_path_csv)
    # Terminate progress line
    print("Finished creating hyperfile")  # terminate line
    
s3_client.upload_file(hyperfile, bucket, hyper_filename)

sns = boto3.client('sns', aws_access_key_id=AWS_PUBLIC_KEY, aws_secret_access_key=AWS_SECRET_KEY, region_name=region)

message = {
            "finished_step": "HyperConverter",
            "schema": options['schema'], 
            "csv": options['csv'], 
            "output": options['output'], 
            "datasource": options['datasource'], 
            "logs": options['logs']
            }

response = sns.publish(
    TopicArn='{TOPIC}',    
    Message=json.dumps({'default': json.dumps(message)}),
    MessageStructure='json'
)

logMessage('[Info] Finished creating hyperfile')
writeLogFile(options['logs'])

