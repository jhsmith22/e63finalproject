5/9/2023
Author: Jessica Stockham

# e63finalproject
This is a my final project for Harvard Extension School e-63 Big Data Analytics. I used pySpark, mySQL, python and Apache Airflow to create a data pipeline with data from the openGSA API. This is primarily a demonstration of Apache Airflow.

## Repository Organization
The code diretory contains all the python scripts. The visualization directory has the html viz created on May 8, 2023. The reports directory contains a full report detailing a full tutorial on how to run this code, a one-page abstract, slides summarizing the project.

## You Tube Video Links
YouTube 2-minute video: https://youtu.be/AYaaRbIt7I4
YouTube 15-minute video: https://youtu.be/CKeJR5OuxSQ

## Problem Statement: 
Explore which federal government agencies have the highest web traffic and which have low traffic and how this varies over time. The federal government provides a vast array of services and information to the American people through its websites. Websites that have little traffic may be poorly designed or not well-publicized. When websites have very high traffic, this may imply that the government must be prepared with resources to meet the high demand for both web traffic and downstream service delivery. In this project, I build a data pipeline, ingesting data in batches from an API weekly, transforming the data with Spark, storing the data in a mySQL database, and visualizing the output with python’s altair package. I orchestrate the workflow with Apache Airflow. 

## High Level Overview of Steps
1.	Install required software (See 'CaseStudyETLSpark_StockhamJessica_Report.docx' for full installation and setup instructions.)
2.	Use python to extract historical website visits data from the analyicts.usa.gov API (Jan 1, 2020 through May 1, 2023) and store the raw data in a mySQL database.
3.	Setup directed acyclic graph in Apache Airflow to do the following steps once a week:
a.	Task 1: Extract the latest website visits data 
b.	Task 2: Transform and store the data so it is ready for analysis.
c.	Task 3: Load transformed data into mysql into python. Visualize using the python altair package. 

## Data Source and Hardware/Software
Dataset Source: analytics.usa.gov API
Hardware: MacOS Monterey Version 12, M1 chip, 16GB
Software: Python 3.9, Spark 3.3.1, Apache Airflow 2.6.0

## Code Files

1. constants.py
The constants.py script gets everything setup. It (1) imports python packages I will need (2) defines variables I use as parameters in my api pulls, and (3) prepares Spark to store the data. I import the python packages: requests, json, pandas, pyspark, and datetime.  For my api parameters, I import my api key that resides in a text file (key.txt), generate of agencies for which I will request reports, define the api url, and also specify the start and end dates (today’s date and the “pull after” date). Notably, the date parameters are updated for each api batch request. I use the datetime python module to calculate today’s date and the date 7 days prior. This code also defines my Spark session object, tests that Spark is working, and defines a schema for my Spark dataframe that will hold the data. 

2. main_api_pull.py
main_api_pull.py is the workhorse that extracts the data from the API. This script imports the constants.py file and defines two functions that pull down the data from the AIP: apiPull() and paginate(). 

apiPull() requires the parameters page, after, before, and agency to feed into the api request. The parameter “page” determines the page number of the data we are pulling from the API. Since the limit for the api is 10,000 data points per page, we must pull multiple pages when we pull more than 10,000 records for a given agency. The parameter “after” represents the “pullAfter” variable defined in the contants.py file (7 days prior to today). The parameter “before” represents the “today” variable defined in the constants.py file (today’s date). The parameter “agency” represents one agency within the list of agencies defined in the constants.py file. 

Within the function, I define the parameters variable, which is a dictionary of all parameters I will feed into the api request. I must use a different api endpoint for each agency. Therefore, I define the endpoint with the agencyReport variable. The response variable holds the results of the api request (url + agencyReport endpoint + parameters), implemented using the python requests package. I return a serialized json object.

paginate() calls the apiPull() function and therefore takes in the parameters apiPull requires in its function signature: after, before, agency.  Paginate() loops through all pages of the api endpoint. 

First, within paginate, I define a blank Spark dataframe, masterDF, that will hold all the data from all pages. The body of the function is a while loop. I use a while loop to continue to execute a loop as long as the last api request returned data (the length of the data variable is not zero).  This allows me to extract all pages of the endpoint. The loop calls the apiPull function and stores the result in the variable “data.” I then execute an “if, else” set of statmements. If the last api request returned no data (the length of the data variable is zero) and I return the Spark dataframe. Else, I store the data in a new Spark dataframe; append that dataframe to the masterDF with the Spark union() function; and finally increment the “page” parameter by 1 to prepare for the next loop.


3. historical_api.py
The historical_api.py script pulls down all website visit data for all federal agencies from the last 5 years: from Jan 1, 2019 through April 30, 2023. This large extraction grabs 3,496,620 rows of data. First, historical_api.py imports constants.py and the main_api_pull.py scripts. Then I loop through all agencies in the agency list, calling the paginate() function for each one. I append all the API data for all agencies in a Spark dataframe called allAgencyDF. Then, I write the allAgencyDF to a mysql database, in a table called “visits” that uses the schema I defined in constants.py.

I run historical_api.py as a script through spark-submit and it takes several minutes to run. I navigate to the folder that holds the scripts so I do not need to write a long path to the script. My terminal command is below.



4. batch_api.py

This is the first script called in the Apache Airflow pipeline DAG (Directed Acyclic Graph). batch_api.py is almost same as historical_api.py, but instead of calling all historical data, it instead dynamically pulls the latest data from the last seven days. This is a script that I will call inside of Apache Airflow to implement the data pipeline weekly . Notably, when I write the new batch data to the mysql database table “visits,” I am “appending” the data because we are appending the batch to the historical data already pulled in historical_api.py. Every weekly batch will keep appending on to this table.

5. transform.py

This is the second script called in the Apache Airflow pipeline DAG. transform.py  loads the raw visits table from the mysql database into a Spark dataframe called df_visits. I transform the raw data to monthly counts so it is ready for analysis and visualization. I use a select statement to group the website visits by month.  For each agency, I use the sum() function to add together all website visits across all website domains for each month. Then I write the transformed data to a new table in the mysql database called monthly_agency. The write function “overwrites” the monthly_agency table because we need to re-aggregate the data with the new data pull. The monthly_agency table has one row for each agency for each month of the year.

6. visualize.py

This is the third script called in the Apache Airflow pipeline DAG. visualize.py loads the data from the mysql database into a pandas dataframe and then visualizes it using the python altair package. First, I installed 3 python packages (PyMySQL, SQLAlchemy, altiar) in to my virtual environment using pip install. I use PyMySQL and SQLAlch to load in the monthly_agency table from mysql database into a pandas dataframe called df.

I also load in the visits table in order to pull out the last date extracted. I leverage the datetime python package to parse the year, month, and day from the string (dt.year, dt.month, dt.day). I use this information to populate the title of the 2023 chart.

Next, I use the pandas dataframe df as the source data for my altair heatmap chart. The heatmap chart is interactive where you can hover over each square to view its data. Notably, I will have to update this code when we get to 2024. A future improvement to this code would make it fully automated so manual coding changes are not needed in future years. I save the altair chart as an html file so it is ready to be served up on a website. 

visualize.py loads the data from the mysql database into a pandas dataframe and then visualizes it using the python altair package. First, I installed 3 python packages (PyMySQL, SQLAlchemy, altiar) in to my virtual environment using pip install. I use PyMySQL and SQLAlch to load in the monthly_agency table from mysql database into a pandas dataframe called df.

I also load in the visits table in order to pull out the last date extracted. I leverage the datetime python package to parse the year, month, and day from the string (dt.year, dt.month, dt.day). I use this information to populate the title of the 2023 chart.

Next, I use the pandas dataframe df as the source data for my altair heatmap chart. The heatmap chart is interactive where you can hover over each square to view its data. Notably, I will have to update this code when we get to 2024. A future improvement to this code would make it fully automated so manual coding changes are not needed in future years. I save the altair chart as an html file so it is ready to be served up on a website.

7. api_dag.py

The api_dag.py is the file used by Apache Airflow to schedule the data pipeline job. I must import several airflow functions, including DAG and BashOperator

The dag_id is ‘api_dag.’ With the first block of code, I schedule this data pipeline to run once a week, every Monday at 17:30 UTC,  defined by the schedule_interval variable. There are five positions in the syntax: minute, hour, day of month, month, day of the week. The syntax here is the same as for the crontab bash utility. The pipeline starts on a date in the past, arbitrarily April 4, 2023, per the start_data variable. Catchup=False means that I don’t want the DAG to go back and pull data from the past.

I define three Tasks in the dag via the BashOperator. The BashOperator allows me to submit bash commands. Task 1 (task_api) runs the batch_api.py script through the spark-submit bash command. Task 2 (task_transform) runs the transform.py script through the spark-submit bash command. Task 3 (task_viz) runs the visualize.py through the python3 bash command. 

I set the order of the three tasks with the last statement with the >> statement, first running task_api, then task_transform, then task_viz.


## Known Bugs
None at this time.


