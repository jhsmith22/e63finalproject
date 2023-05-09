#!/usr/bin/env python
# coding: utf-8

import requests
import json
import pandas as pd
import pyspark
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

''' The constants.py file defines the parameters that we will use in the api pull.
 This includes the api key, the list of federal agencies for which we will request data, 
 the date range (today and 7 days in the past). 
 The api documentation is at https://open.gsa.gov/api/dap/'''

# Api key
f = open('/Users/Jess/Documents/e63/apiPull/key.txt', 'r')
key = f.read()

# Agencies to query
agencies = ['agency-international-development',
            'agriculture',
            'commerce',
            'defense',
            'education',
            'energy',
            'environmental-protection-agency',
            'executive-office-president',
            'general-services-administration',
            'health-human-services',
            'homeland-security',
            'housing-urban-development',
            'interior',
            'justice',
            'labor',
            'national-aeronautics-space-administration',
            'national-archives-records-administration',
            'national-science-foundation',
            'nuclear-regulatory-commission',
            'office-personnel-management',
            'postal-service',
            'small-business-administration',
            'social-security-administration',
            'state',
            'transportation',
            'treasury',
            'veterans-affairs']

# API URL
url = 'https://api.gsa.gov/analytics/dap/v1.1'

# pullAfter: This defines the timeframe of the batch data pull (go back 7 days)
# For example, if today is May 4 when we execute the code, the pullAfter date is April 27.
# We pull down data from the API from April 27 - May 3.
pullAfter = datetime.now() - timedelta(7)
pullAfter = datetime.strftime(pullAfter, '%Y-%m-%d')
print(f'Two Days Ago: {pullAfter}')

# TODAY'S DATE
today = datetime.today().strftime('%Y-%m-%d')
print(f'Today: {today}')

# Schema for Spark Dataframe. Will hold data from API
from pyspark.sql.types import *

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("date", StringType(), True),
    StructField("report_name", StringType(), True),
    StructField("report_agency", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("visits", IntegerType(), True)
])

print("Loaded Constants")

# Spark Session Object
spark = SparkSession.builder.getOrCreate()

# Show that pyspark is working by printing out "Hello Spark" in a dataframe
df = spark.sql("select 'spark' as hello ")
df.show()

print("Spark is Setup")
