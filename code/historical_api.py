#!/usr/bin/env python
# coding: utf-8

# Step 1: Load constant variables
from constants import *

# Step 2: Load functions we will use to pull down data from API
from main_api_pull import *

# Step 3: Extract historical data from API: Jan 1, 2019 - April 30, 2023
# Instantiate Blank DF to hold all data
allAgencyDF = spark.createDataFrame(data=[], schema=schema)

# Loop through all agencies
for agency in agencies:
    # Pull Data
    agencyDF = paginate(after='2019-01-01', before='2023-04-30', agency=agency)
    print(f'agency count: {agencyDF.count()}')
    allAgencyDF = allAgencyDF.union(agencyDF)

allAgencyDF.count()

# Write data to "visits" table in mysql database
allAgencyDF.write \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306") \
    .option("dbtable", "websites.visits") \
    .option("user", "root") \
    .option("password", "password") \
    .mode("overwrite").save()


