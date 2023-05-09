# Step 1: Load constant variables
from constants import *

# Step 2: Load functions we will use to pull down data from API
from main_api_pull import *

# Step 3: Pull All Historical Data

# Instantiate Blank DF to hold all data
allAgencyDF = spark.createDataFrame(data = [], schema = schema)

# Loop thorugh all agencies
for agency in agencies:

    # Pull All Data From the Last 7 Days
    agencyDF = paginate(after=pullAfter, before=today, agency=agency)
    print(f'agency count: {agencyDF.count()}')

    allAgencyDF = allAgencyDF.union(agencyDF)

allAgencyDF.count()

#W rite data to "visit" table in mysql database
allAgencyDF.write     \
    .format("jdbc")     \
    .option("url","jdbc:mysql://localhost:3306")     \
    .option("dbtable", "websites.visits")     \
    .option("user", "root")     \
    .option("password", "password")    \
    .mode("append").save()

