# -*- coding: utf-8 -*-
import dataiku
from dataiku import spark as dkuspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import *
from pyspark.ml.feature import *
import numpy as np

sc = SparkContext()
sqlContext = SQLContext(sc)

# Recipe inputs
# Example: Read the descriptor of a Dataiku dataset
data = dataiku.Dataset("sn_test_new_prepared")
# And read it as a Spark dataframe
df = dkuspark.get_dataframe(sqlContext, data)

df_part = df[['collector_tstamp', 'event', 'domain_userid', 'domain_sessionidx', 'network_userid', 'page_title',
             'Brand', 'Sub-Category', 'Super Category', 'Country']]

time_delta = (max(unix_timestamp(col('collector_tstamp'))) 
              - min(unix_timestamp(col('collector_tstamp')))).alias("time_delta")
pages = count(when(col('event') == 'page_view', True)).alias('pages')
events = count(col('event')).alias('events')
sessions = count(col('domain_sessionidx')).alias('sessions')

# to define sub-df: domain_userid, domain_sessionidx, Brand
group_data = df_part.groupby('domain_userid', 'domain_sessionidx', 'Brand').agg(time_delta , pages, events, sessions)

udf = UserDefinedFunction(lambda x: float(np.log(x+1)), FloatType())
group_data = group_data.withColumn('norm_time_delta', udf(group_data.time_delta))
group_data = group_data.withColumn('norm_pages', udf(group_data.pages))
group_data = group_data.withColumn('norm_events', udf(group_data.events))
group_data = group_data.withColumn('norm_sessions', udf(group_data.sessions))

# vectorAssembler = VectorAssembler(inputCols=["time_delta", "pages", "events"],
#                                  outputCol="features")
# For your special case that has string instead of doubles you should cast them first.
# expr = [log10(col(c)).alias(c) 
#          for c in vectorAssembler.getInputCols()]
# group_data = group_data.select(*expr)

# group_data2 = vectorAssembler.transform(group_data)
# mmScaler = MinMaxScaler(inputCol="features", outputCol="scaled_features")
# model = mmScaler.fit(group_data2)
# group_data2 = model.transform(group_data2)
# features = group_data.map(lambda x: x.events)
# scaler1 = StandardScaler().fit(features)
# scaler2 = StandardScaler(withMean=True, withStd=True).fit(features)

max_norm_timedelta = group_data.groupby().max('norm_time_delta').collect()[0].asDict()['max(norm_time_delta)']
max_norm_pages = group_data.groupby().max('norm_pages').collect()[0].asDict()['max(norm_pages)']
max_norm_events = group_data.groupby().max('norm_events').collect()[0].asDict()['max(norm_events)']
max_norm_sessions = group_data.groupby().max('norm_sessions').collect()[0].asDict()['max(norm_sessions)']
min_norm_timedelta = group_data.groupby().min('norm_time_delta').collect()[0].asDict()['min(norm_time_delta)']
min_norm_pages = group_data.groupby().min('norm_pages').collect()[0].asDict()['min(norm_pages)']
min_norm_events = group_data.groupby().min('norm_events').collect()[0].asDict()['min(norm_events)']
min_norm_sessions = group_data.groupby().min('norm_sessions').collect()[0].asDict()['min(norm_sessions)']

def calc_score(group_data):
    new_norm_timedelta = (group_data.norm_time_delta - min_norm_timedelta)/(max_norm_timedelta - min_norm_timedelta)
    new_norm_pages = (group_data.norm_pages - min_norm_pages)/(max_norm_pages - min_norm_pages)
    new_norm_events = (group_data.norm_events - min_norm_events)/(max_norm_events - min_norm_events)
    new_norm_sessions = (group_data.norm_sessions - min_norm_sessions)/(max_norm_sessions - min_norm_sessions)
    score = (new_norm_timedelta + new_norm_events + new_norm_sessions)*10/3
    return score

#aff_score = udf(lambda x: calc_score(x), IntegerType())
group_data = group_data.withColumn("affinity_score", calc_score(group_data))
group_data = group_data.withColumnRenamed('domain_userid', 'domain_userid2')
group_data = group_data.withColumnRenamed('domain_sessionidx', 'domain_sessionidx2')
group_data = group_data.withColumnRenamed('Brand', 'Brand2')

cond = [df_part.domain_userid == group_data.domain_userid2, df_part.domain_sessionidx == group_data.domain_sessionidx2, 
       df_part.Brand == group_data.Brand2]
group_df = df_part.join(group_data, cond, 'inner')

group_df_dedup = group_df.select([c for c in group_df.columns if c not in {'event', 'page_title', 'domain_userid2','domain_sessionidx2',
                                                                           'Brand2'}]).dropDuplicates(['domain_userid','domain_sessionidx',
                                                                                                       'Brand'])


# We have calculated an affinity score per session. Now we need to calculate one per Brand/Sub C/Super C

brand_score = format_number(avg(col('affinity_score')), 2).alias("brand_score")
brand_timedelta = format_number(avg(col('time_delta')), 2).alias("brand_timedelta")
brand_events = format_number(avg(col('events')), 2).alias("brand_events")
brand_sessions = count(col('domain_sessionidx')).alias("brand_sessions")
group_brand = group_df_dedup[['collector_tstamp', 'domain_userid', 'domain_sessionidx', 'network_userid',
                           'Brand', 'Sub-Category', 'Super Category', 'Country', 'time_delta', 'pages',
                           'events', 'affinity_score']].groupby('network_userid', 'Sub-Category', 'Brand').agg(brand_score,
                                                                                                               brand_sessions, 
                                                                                                               brand_timedelta,
                                                                                                               brand_events)

subcat_score = format_number(avg(col('affinity_score')), 2).alias("subcat_score")
subcat_timedelta = format_number(avg(col('time_delta')), 2).alias("subcat_timedelta")
subcat_events = format_number(avg(col('events')), 2).alias("subcat_events")
subcat_sessions = count(col('domain_sessionidx')).alias("subcat_sessions")
group_subcat = group_df_dedup[['collector_tstamp', 'domain_userid', 'domain_sessionidx', 'network_userid',
                           'Brand', 'Sub-Category', 'Super Category', 'Country', 'time_delta', 'pages',
                           'events', 'affinity_score']].groupby('network_userid', 'Super Category', 'Sub-Category').agg(subcat_score,
                                                                                                                        subcat_sessions,
                                                                                                                        subcat_timedelta,
                                                                                                                        subcat_events)

supcat_score = format_number(avg(col('affinity_score')), 2).alias("supcat_score")
supcat_timedelta = format_number(avg(col('time_delta')), 2).alias("supcat_timedelta")
supcat_events = format_number(avg(col('events')), 2).alias("supcat_events")
supcat_sessions = count(col('domain_sessionidx')).alias("supcat_sessions")
group_supcat = group_df_dedup[['collector_tstamp', 'domain_userid', 'domain_sessionidx', 'network_userid',
                           'Brand', 'Sub-Category', 'Super Category', 'Country', 'time_delta', 'pages',
                           'events', 'affinity_score']].groupby('network_userid', 'Super Category').agg(supcat_score,
                                                                                                        supcat_sessions, 
                                                                                                        supcat_timedelta,
                                                                                                        supcat_events)
group_subcat = group_subcat.withColumnRenamed('network_userid', 'network_userid2')
group_subcat = group_subcat.withColumnRenamed('Super Category', 'Super Category2')
group_brand = group_brand.withColumnRenamed('network_userid', 'network_userid3')
group_brand = group_brand.withColumnRenamed('Sub-Category', 'Sub-Category2')

group_cat = group_supcat.join(group_subcat, [group_supcat['network_userid'] == group_subcat['network_userid2'],
                                             group_supcat['Super Category'] == group_subcat['Super Category2']],
                                'outer')
group_cat_brand = group_cat.join(group_brand, [group_cat['network_userid'] == group_brand['network_userid3'],
                                               group_cat['Sub-Category'] == group_brand['Sub-Category2']],
                                'outer')
# new columns: timeonPage, avgPages, affinityScore, categorySessionidx (to count number of sessions within Category)
# and keep: domain_userid, domain_sessionidx, network_userid


# Recipe outputs
sn_test_new_joined2 = dataiku.Dataset("sn_test_new_joined2")
dkuspark.write_with_schema(sn_test_new_joined2, group_cat_brand)