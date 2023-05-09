import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, column, desc, col, year, month, dayofmonth

spark = SparkSession.builder.getOrCreate()

# Show that pyspark is working
df = spark.sql("select 'spark' as hello ")
df.show()

# Read in visits data from websites database
df_visits = spark.read \
    .format("jdbc") \
    .option("url","jdbc:mysql://localhost:3306") \
    .option("dbtable", "websites.visits") \
    .option("user", "root") \
    .option("password", "password") \
    .load()

df_visits.show(1)

# Aggregate Table: Agency Visits by Month
monthly_agency = df_visits\
.select(
    "report_agency", "visits", "date",
    year("date").alias('year'),
    month("date").alias('month'),
    dayofmonth("date").alias('day'))\
.groupBy(col("report_agency"), col("year"), col("month"))\
.sum("visits").withColumnRenamed("sum(visits)", "visits_agg").orderBy("report_agency", "year","month")

monthly_agency.show(1)

# Save transformed table into database
# Over-write stored data
monthly_agency.write \
    .format("jdbc") \
    .option("url","jdbc:mysql://localhost:3306") \
    .option("dbtable", "websites.monthly_agency") \
    .option("user", "root") \
    .option("password", "password")\
    .mode("overwrite").save()