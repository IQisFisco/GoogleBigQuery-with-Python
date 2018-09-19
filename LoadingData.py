# -*- coding: utf-8 -*-
"""
Created on Sat Sep 15 02:43:30 2018

@author: IEUser
"""

import json

from google.cloud import bigquery
from google.cloud.bigquery import Dataset
from google.cloud.bigquery import LoadJobConfig
from google.cloud.bigquery import SchemaField
from google.cloud import storage

#Defining constants
path_key_path = "C:/Users/IEUser/Documents/auth_gc_bq.json"
file_name = "C:/Users/IEUser/Documents/cars dataset.csv"
dataset_id = "myDS6"
table_id = "myTab6"

#Reading json file for connectivity to BigQuery 
auth = json.loads(open(path_key_path).read())
key_path = auth['key_path']
storage_client = storage.Client.from_service_account_json(key_path)

#Connecting to BigQuery
client = bigquery.Client.from_service_account_json(key_path)

#Setting up dataset and tables for loading the data
dataset_ref = client.dataset(dataset_id)
dataset = Dataset(dataset_ref)
dataset = client.create_dataset(dataset)
table_ref = dataset_ref.table(table_id)

#Defining schema of the table
SCHEMA = [
    SchemaField('Origin', 'STRING', mode='required'),
    SchemaField('Size', 'STRING', mode='required'),
    SchemaField('Type', 'STRING', mode='required'),
    SchemaField('Home', 'STRING', mode='required'),
    SchemaField('Income', 'STRING', mode='required'),
    SchemaField('Marital', 'STRING', mode='required'),
    SchemaField('Kids', 'INTEGER', mode='required'),
    SchemaField('Sex', 'STRING', mode='required'),
    SchemaField('RowNumber', 'INTEGER', mode='required'),
]

#SCHEMA = [
#    SchemaField('Name', 'STRING', mode='required'),
#    SchemaField('Age', 'INTEGER', mode='required'),
#]

#setting up job for loading
load_config = LoadJobConfig()
load_config.source_format = bigquery.SourceFormat.CSV
load_config.skip_leading_rows = 1
load_config.schema = SCHEMA

with open(file_name, "rb") as readable:
    job = client.load_table_from_file(
                readable,
                table_ref,
                job_config = load_config)

job.result()

print("Loaded {} rows into {}:{}.".format(
        job.output_rows, dataset_id, table_id))