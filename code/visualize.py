from sqlalchemy import create_engine
import pymysql
import altair as alt
import pandas as pd
from datetime import datetime

# Connect to mysql database
mysql_string = 'mysql+pymysql://root:password@localhost:3306/websites'
conn = create_engine(mysql_string)
df = pd.read_sql('SELECT * FROM monthly_agency', con=conn)

# Get Last Day of Data
df_raw = pd.read_sql('SELECT date FROM visits  order by date desc limit 1', con=conn)
datestring = df_raw.iloc[0,0]
dt = datetime.strptime(datestring, '%Y-%m-%d')
print(dt.year, dt.month, dt.day)

# Create Heatmap and save as html file
# Will need to update the code when we get to 2024

tooltip=[alt.Tooltip('report_agency:O', title = 'Agency'),
         alt.Tooltip('year:O', title = "Year"),
         alt.Tooltip('month:O', title = 'Month'),
         alt.Tooltip('visits_agg:Q', title = 'Monthly Website Visits', format=",.0f")]

chart2019 = alt.Chart(df.loc[df.year==2019], title="Monthly Website Visits in 2019").mark_rect().encode(
    x = alt.X("month:O", title="Month"),
    y = alt.Y("report_agency:O", title="Agency"),
    tooltip = tooltip,
    color = alt.Color("visits_agg", legend=alt.Legend(title='Monthly Website Visits', format=",.0f")))

chart2020 = alt.Chart(df.loc[df.year==2020], title="2020").mark_rect().encode(
    x = alt.X("month:O", title="Month"),
    y=alt.Y("report_agency:O", title="Agency", axis = alt.Axis(labels=False)),
    tooltip =tooltip,
    color=alt.Color("visits_agg"))

chart2021 = alt.Chart(df.loc[df.year==2021], title="2021").mark_rect().encode(
    x = alt.X("month:O", title="Month"),
    y=alt.Y("report_agency:O", title="Agency", axis = alt.Axis(labels=False)),
    tooltip =tooltip,
    color=alt.Color("visits_agg"))


chart2022 = alt.Chart(df.loc[df.year==2022], title="2022").mark_rect().encode(
    x = alt.X("month:O", title="Month"),
    y=alt.Y("report_agency:O", title="Agency", axis = alt.Axis(labels=False)),
    tooltip =tooltip,
    color=alt.Color("visits_agg"))

chart2023 = alt.Chart(df.loc[df.year==2023], title="2023 through " + str(dt.month) + '/' + str(dt.day) + '/' + str(dt.year)).mark_rect().encode(
    x = alt.X("month:O", title="Month"),
    y=alt.Y("report_agency:O", title="Agency", axis = alt.Axis(labels=False)),
    tooltip =tooltip,
    color=alt.Color("visits_agg"))

heatmap = chart2019 | chart2020 | chart2021 | chart2022 | chart2023
heatmap.save('/Users/Jess/Documents/e63/apiPull/heatmap.html')