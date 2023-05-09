#!/usr/bin/env python
# coding: utf-8

# Step 1: Load constant variables
from constants import *

# Step 2: Define functions to pull data from API
def apiPull(page, after, before, agency):
    """Request data from api"""
    
    """
    Parameters: 
    - page: page number of the request
    - after: data after which we are requesting data
    -agency: federal agency for which we are requesting data
    """
    
    # Parameters used in the api request
    parameters = {
    "limit": 10000,  # Limit is 10,000
    "page" : page,
    "after": after,
    "before": before,
    "api_key": key}
    
    # Endpoint
    agencyReport = '/agencies/' + agency + '/reports/site/data'
    
    # Request data
    response = requests.get(url + agencyReport, params=parameters)

    print(f'response status code: {response.status_code}')

    # Return a list of json objects
    return response.json()


def paginate(after, before, agency):
    """Loop through all pages until we have all requested data for that agency"""    
    """
    Parameters: 
    - after: data after which we are requesting data
    -agency: federal agency for which we are requesting data
    """

    masterDF = spark.createDataFrame(data = [], schema = schema)    # initialize blank df
    page = 1 # starting place
    data = [1] # placeholder
    
    # If the request returned data, keep going
    while len(data) != 0:
        data = apiPull(page, after, before, agency)
        print(masterDF.count())
        
        # If the length of the data returned is 0, stop and return df
        if len(data) == 0:
            print(f'done with {agency}')
            return masterDF
        
        # If the data has a length > 0, append that data to the masterDF
        # Increment the page count by 1
        else:

            df = spark.createDataFrame(data, schema)
            masterDF = masterDF.union(df)
            page += 1
