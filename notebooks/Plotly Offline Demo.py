# Databricks notebook source
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
userhome = 'dbfs:/user/' + username
print(userhome)
scratch_path = userhome + "/scratch/" 
scratch_path_open = '/dbfs' + scratch_path.split(':')[-1] # for use with python open()
dbutils.fs.mkdirs(scratch_path)
scratch_path

# COMMAND ----------

dbutils.fs.ls(scratch_path)

# COMMAND ----------

# MAGIC %sh 
# MAGIC pip install plotly --upgrade
# MAGIC pip install chart_studio --upgrade
# MAGIC pip install nbformat --upgrade

# COMMAND ----------

# dbutils.fs.put(scratch_path+"/test.json", """
# {"string":"string1","int":1,"array":[1,2,3],"dict": {"key": "value1"}}
# {"string":"string2","int":2,"array":[2,4,6],"dict": {"key": "value2"}}
# {"string":"string3","int":3,"array":[3,6,9],"dict": {"key": "value3", "extra_key": "extra_value3"}}
# """, True)

testJsonData = sqlContext.read.json(scratch_path+"/btd2.json")

display(testJsonData)

# COMMAND ----------

print(type(testJsonData))

# COMMAND ----------

testJsonData.printSchema()

# COMMAND ----------

testJsonData.take(3)

# COMMAND ----------

# MAGIC %md
# MAGIC To answer that we'll get the durations and the way we'll be doing it is through the Spark SQL Interface. To do so we'll register it as a table.

# COMMAND ----------

sqlContext.registerDataFrameAsTable(testJsonData, "bay_area_bike")

# COMMAND ----------

# MAGIC %md
# MAGIC Now as you may have noted above, the durations are in seconds. Let's start off by looking at all rides under 2 hours.

# COMMAND ----------

df2 = sqlContext.sql("SELECT Duration as d1 from bay_area_bike where Duration < 7200")


# COMMAND ----------

#data = [go.Histogram(x=df2.toPandas()['d1'])]

# COMMAND ----------

from plotly.offline import plot
from plotly.graph_objs import *
import numpy as np

x = np.random.randn(2000)
y = np.random.randn(2000)

# Instead of simply calling plot(...), store your plot as a variable and pass it to displayHTML().
# Make sure to specify output_type='div' as a keyword argument.
# (Note that if you call displayHTML() multiple times in the same cell, only the last will take effect.)

p = plot(
  [
    Histogram(x=df2.toPandas()['d1'])
  ],
  output_type='div'
)

displayHTML(p)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC That was simple and we can see that plotly was able to handle the data without issue. We can see that big uptick in rides that last less than ~30 minutes (2000 seconds) - so let's look at that distribution.

# COMMAND ----------

df3 = sqlContext.sql("SELECT Duration as d1 from bay_area_bike where Duration < 2000")

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC A great thing about Apache Spark is that you can sample easily from large datasets, you just set the amount you would like to sample and you're all set. Plotly converts those samples into beautifully overlayed histograms. This is a great way to eyeball different distributions.

# COMMAND ----------

s1 = df2.sample(False, 0.05, 20)
s2 = df3.sample(False, 0.05, 2500)
p = plot(
  [
    Histogram(x=s1.toPandas()['d1'], name="Large Sample"),
    Histogram(x=s2.toPandas()['d1'], name="Small Sample")
  ],
  output_type='div'
)

displayHTML(p)


# COMMAND ----------

# MAGIC %md
# MAGIC Now let's check out bike rentals from individual stations. We can do a groupby with Spark DataFrames just as we might in Pandas. We've also seen at this point how easy it is to convert a Spark DataFrame to a pandas DataFrame.

# COMMAND ----------

dep_stations = testJsonData.groupBy(testJsonData['Start Station']).count().toPandas().sort('count', ascending=False)
dep_stations['Start Station'][:3] # top 3 stations

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC we'll add a handy function to help us convert all of these into appropriate count data. We're just using pandas resampling function to turn this into day count data.

# COMMAND ----------

def transform_df(df):
    df['counts'] = 1
    df['Start Date'] = df['Start Date'].apply(pd.to_datetime)
    return df.set_index('Start Date').resample('D', how='sum')

pop_stations = [] # being popular stations - we could easily extend this to more stations
for station in dep_stations['Start Station'][:3]:
    temp = transform_df(btd.where(btd['Start Station'] == station).select("Start Date").toPandas())
    pop_stations.append(
        go.Scatter(
        x=temp.index,
        y=temp.counts,
        name=station
        )
    )

data = pop_stations
#py.iplot(data, filename="spark/over_time")


p = plot(
  data,
  output_type='div'
)

displayHTML(p)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC https://plot.ly/python/v3/apache-spark/

# COMMAND ----------

