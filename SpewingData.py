# -*- coding: utf-8 -*-
"""
Created on Sat Sep 15 12:07:08 2018

@author: IEUser
"""
import json
import pandas as pd
import pandas_gbq as pd_gbq
import scipy.stats as stats
from google.cloud import bigquery
from google.cloud import storage

path_key_path = "C:/Users/IEUser/Documents/auth_gc_bq.json"

#Reading json file for connectivity to BigQuery 
auth = json.loads(open(path_key_path).read())
key_path = auth['key_path']
storage_client = storage.Client.from_service_account_json(key_path)

#Connecting to BigQuery
client = bigquery.Client.from_service_account_json(key_path)

#Creating the query to extract the dataframe
QUERY = (
    'SELECT Origin, Marital '
    'FROM myDS6.myTab6 '
    )
#Creating the datafrane
df = pd_gbq.read_gbq(QUERY,
                     project_id = "my-big-query-project-216510",
                     private_key = "C:/Users/IEUser/Documents/My big query project-3ac1309ade50.json",
                     dialect = "standard",
                     verbose = False)

#Calculating statistics for independence
pd.options.display.max_columns = 9999
df_crossed = pd.crosstab(df.Origin, df.Marital, margins = True)
df_crossed.columns = ["Married", "Married with Kids", "Single", "Single with Kids", "Origin Totals"]
df_crossed.index = ["American", "European", "Japanese", "Marital Totals"]
print("======Cross Tabulated Table=======")
print(df_crossed)

chi2, p, dof, expected = stats.chi2_contingency(observed = df_crossed)
print("Chi Square Value is {0:.4f}".format(chi2))
print("p-value is {0:.4f}".format(p))
print("where alpha = 0.5")





