# -*- coding: utf-8 -*-
import dataiku
from dataiku import spark as dkuspark
from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *

sc = SparkContext()
sqlContext = SQLContext(sc)
hive = HiveContext(sc)

# unpack array of dates or pages
def unpack(row):
    res = []
    for i in row[1]:
        res.append(i)
    return (row[0], res)

# calculate session duration
def tim_dif(l):
    res = []
#  diff_in_secs = -1
    if l[1]:
        for i in l[1]:
            k = i[0:-4]
            try:
                j = datetime.datetime.strptime(k, "%Y-%m-%d %H:%M:%S")
                res.append(j)
            except ValueError:
                pass
    if len(res)>1:
        diff = max(res) - min(res)
        diff_in_secs = (diff.seconds + diff.days * 24 * 3600)
    else:
        diff_in_secs = 0
    return (l[0], diff_in_secs)

def process_row(l):
    # # line is a list object at this stage
    # # load master data maps
    # country_dict = U.value
    # category_dict = V.value
    # brand_dict = B.value
    # xref_dict = X.value
    # # now id brands, cat, country
    # found_brand = False
    # found_cat = False
    # found_country = False
    # cat_code = ""
    # country_code = ""
    # brand_code = ""
    # # check country
    # item = l[127].encode('utf-8')
    # for country in country_dict:
    #             if item == country:
    #                 found_country = True
    #                 country_code = country_dict.get(country)
    # # check cat
    # item = l[126].encode('utf-8')
    # for cat in category_dict:
    #             if item == cat:
    #                 found_cat = True
    #                 cat_code = category_dict.get(cat)
    #         # check brands
    # item = l[125].encode('utf-8')
    # for brand in brand_dict:
    #             if item == brand:
    #                 found_brand = True
    #                 brand_code = brand_dict.get(brand)
    # # also check that categories have been correctly identified from brand
    # newMap = X.value
    # matched = False
    # for i in newMap:
    #     if i[0] == brand_code:
    #         # then i[1] is a list of the cat codes for this brand
    #         for item in i[1]:
    #           if item == cat_code:
    #             matched = True
    # 
    # # check data has valid cell attributes
    # if found_brand and found_cat and found_country and matched:
    #         error_flag = ""
    # else:
    #         error_flag = "Y"
    # # ensure bad data is not saved - keep valid identified data
    # # leave unidentified cells empty
    # if not found_country:
    #         county_code = ""
    # if not found_brand:
    #         brand_code = ""
    # if not found_cat:
    #         cat_code = ""
    # cell_fields = [country_code, cat_code, brand_code]
    
    # start forming output row in format:
    # [ year, MMYY, dates, original rows, metrics]
    w = W.value
    t = T.value
    p = P.value
    metrics = []
    metrics.append(l[0])
    metrics.append(str(t.get(l[0])))
    metrics.append(p.get(l[0]))
    output_url =""
    for i in w.get(l[0]):
                if i[0:3] == 'www':
                  output_url = i
    metrics.append(output_url.encode('utf-8'))
    # errors = [error_flag]
    # form date fields required
    temp_date = l[3][0:10]
    temp_year = temp_date[0:4]
    temp_mmyy = temp_date[5:7] + temp_date[2:4]
    date_fields = [temp_year, temp_mmyy, temp_date]
    # form full output row
    dash_output = date_fields + metrics + l
    return dash_output

# Main method

test_snow = sc.textFile("/sources/snowplow/2016-04-*")
#test_snow = sc.textFile("/sources/snowplow/*/part-*")
#print("test_snow")
#print(test_snow.count())
#print("test_snow count")
#temp = test_snow.map(lambda r: r.split("\t"))
#print("temp")
#print(temp.count())
#print("temp count")
#temp2 = temp.filter(lambda x: len(x) >= 108)
#print("temp2")
#print(temp2.count())
#print("temp2 count")
#temp3 = temp2.map(lambda x: x[0:108])
#print("temp3")
#print(temp3.count())
#print("temp3 count")
# temp2 = cols.filter(lambda x: len(x) == 131)
snow_temp = test_snow.map(lambda r: r.split("\t")).filter(lambda x: len(x) >= 108).map(lambda x: x[0:108])
print("snow_temp")
print(snow_temp.count())
print("snow_temp count")

# temp3 = temp2.map(lambda x: flatten(x))
# temp4 = temp3.filter(lambda x: len(x) == 128)

# # calculates session duration
# session_times = temp4.map(lambda x: (x[0],x[4]))
# grouped_session_times = session_times.groupByKey()
# unpacked_session_times = grouped_session_times.map(lambda x: unpack(x))
# unpacked_session_times2 = unpacked_session_times.filter(lambda x: x[0] != "")
# session_timediff = unpacked_session_times2.map(lambda x: tim_dif(x))
# t = session_timediff.collectAsMap()
# T = sc.broadcast(t)
# 
# # number of pages visited per session
# pages_count = temp4.map(lambda x: (x[0],x[16]))
# grouped_pages_count = pages_count.groupByKey()
# unpacked_pages_count = grouped_pages_count.map(lambda x: unpack(x))
# pages_max_count = unpacked_pages_count.map(lambda x:(x[0], max(x[1])))
# p = pages_max_count.collectAsMap()
# P = sc.broadcast(p)
# 
# # generates list of web pages visited in a session
# domain_count = temp4.map(lambda x: (x[0],x[33]))
# grouped_domain_count = domain_count.groupByKey()
# unpacked_domain_count = grouped_domain_count.map(lambda x: unpack(x))
# w = grouped_domain_count.collectAsMap()
# W = sc.broadcast(w)

# remove screwed up data
#        clean_rows = temp3.filter(lambda x: len(x) == 128)
#        bad_rows = cols2.filter(lambda x: len(x) != 128)

# processed_rows = temp4.map(lambda x : process_row(x))
# form output schema and write out RDD
# good_data = processed_rows.filter(lambda x: x[-1] == '')
# error_data = processed_rows.filter(lambda x: x[-1] == 'Y')

# snowplowSchemaString = [app_id platform etl_tstamp collector_tstamp dvce_tstamp event event_id txn_id name_tracker v_tracker v_collector v_etl user_id use_ipaddress user_fingerprint domain_userid domain_sessionidx network_userid geo_country geo_region geo_city geo_zipcode geo_latitude geo_longitude geo_region_name ip_isp ip_organization ip_domain ip_netspeed page_url page_title page_referrer page_urlscheme page_urlhost page_urlport page_urlpath page_urlquery page_urlfragment refr_urlscheme refr_urlhost refr_urlport refr_urlpath refr_urlquery refr_urlfragment refr_medium refr_source refr_term mkt_medium mkt_source mkt_term mkt_content mkt_campaign unilever_json se_category se_action se_label se_property se_value unstruct_event tr_orderid tr_affiliation tr_total tr_tax tr_shipping tr_city tr_state tr_country ti_orderid ti_sku ti_name ti_category ti_price ti_quantity pp_xoffset_min pp_xoffset_max pp_yoffset_min pp_yoffset_max useragent br_name br_family br_version br_type br_renderengine br_lang br_features_pdf br_features_flash br_features_java br_features_director br_features_quicktime br_features_realplayer br_features_windowsmedia br_features_gears br_features_silverlight br_cookies br_colordepth br_viewwidth br_viewheight os_name os_family os_manufacturer os_timezone dvce_type dvce_ismobile dvce_screenwidth dvce_screenheight doc_charset doc_width doc_height temp01 temp02 temp03 temp04 temp05 temp06 temp07 temp08 temp09 temp10 temp11 temp12 temp13 temp14 temp15 temp16 temp17]
parameters = hive.sql("SELECT * FROM mtm.xxhvmt91")
snowplowSchemaString = parameters.filter("parameter = 'sn_schema_string'").map(lambda x: x[3]).first()
temp_fields = [field for field in snowplowSchemaString.split()[10:]]
# fields_to_add = ['temp24', 'temp25', 'temp26', 'temp27', 'temp28', 'temp29']
temp_fields = temp_fields[:-21]
fields = [StructField(field, StringType(), True) for field in temp_fields]
# fields = [StructField(field, StringType(), True) for field in snowplowSchemaString.split()[10:]]
# fields = [field for field in snowplowSchemaString.split()[10:]]
print('fields')
print(len(fields))

# write good data
# goodDataTable = hive.applySchema(temp2, StructType(fields))
# goodDataTable.saveAsTable("snowplow_test")
sn_df = sqlContext.createDataFrame(snow_temp, StructType(fields))
print("sn_df")
print(type(sn_df))
if sn_df.rdd.isEmpty:
    print('True')
else: 
    print('False')

    
# Recipe outputs
sn_targeting_new = dataiku.Dataset("sn_targeting_hive")
dkuspark.write_with_schema(sn_targeting_new, sn_df)

