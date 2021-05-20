
####Layer 1

# Import File to Match as df
# Import Databse to Match as db

### Join based on Entity Name
# Changing some Datatypes from String to Floats and Longs
from pyspark.sql.types import LongType, FloatType
import pyspark.sql.functions as f
from pyspark.sql.functions import lower, col, lit, when
from pyspark.sql import Window
import pyspark.sql.functions as f
import pyspark.sql.functions as f
from pyspark.sql.functions import lower, col
import pandas as pd
import numpy as np


###Lower case all
db = db.withColumn("BusinessName", lower(col("BusinessName")))
df = df.withColumn("InsName_layer1", lower(col("InsName")))


# count distinct business names in df
x = df.select("InsName").distinct()


##################### Standard Join Pre-Processing ##############################

### Extract simple join match
#Inner join on business/insured name

Matched_companies_01 = db.join(df, (df.InsName_layer1 == db.BusinessName))

######01_Creating final DB
b = Matched_companies_01.filter(Matched_companies_01['Revenue'] > 0)

w = Window.partitionBy('InsName')
Layer_1 = b.withColumn('maxRevenue', f.max('Revenue').over(w))\
    .where(f.col('Revenue') == f.col('maxRevenue'))\
    .drop('maxRevenue')

Layer_1 = Layer_1.select("Pol_Num","BusinessName","InsName","Revenue","SICCode","NAICSCode","EmployeesTotal","State","City","ZIP_Code")

display(Layer_1)

# DBTITLE 1,Layer_2 Fuzzy Match
####### Fuzzy Match ################

Step2 = df
##USE
Step2 = Step2.select("Pol_Num","InsName","State","City","ZIP_Code").distinct()

##################### Filtered Join Post-Processing ##############################

####RUN 4
### STRING PROCESSING MANUAL ###       ####RUN####


# string processing only one column in a df
# make select a column. x acts as the complete table
x = Step2.select(col("InsName"))
# add the a column with the processed text. the resulting table is finale
finale = x.withColumn("InsLow", lower(col("InsName")))
# remove the pre-processed column from the table. the result is the original table where one column has been replaced by a processed version of itself. this is y
y = finale.drop("InsName")

# lowercase all values in a column
low = Step2.select(lower(col("InsName")))

# remove multi-character substrings like '.com'
nosub = Step2.withColumn('new', f.regexp_replace('InsName', '.com', ''))

# remove all occurrences of individual characters. inputting multiple characters removes all occurrences of each character
noind = Step2.select("InsName", f.translate(f.col("InsName"), "!@#$%^&*()_+/<>:;'.,'", "").alias("replaced"))

##########################################################

### JOIN STRATEGY ###

# I : filter the strings:
# 1 : make InsName column of df all lowercase
# 2 : remove substrings: .com_ , .net_ , .org_
# 3 : remove common stopwords including _ltd , _corp, _and
#           remember to remove stopwords either prefaced or suffixed with _ so they are not removed from whole words
#           need to come up with strategy to handle cases like "my company co"
#           remove _inc. and _inc
# 4 : remove special characters including . and ,
# 5 : remove blanks

# II : Join the tables:
# 1 : apply same process as above to filter the text of both dbBusiness.Name
# 2 : join (exact match) df and db on the filtered InsName and BusinessName fields

##################### df STRING PROCESSING ##############################

####RUN 5
## Remove Web suffixes            ####RUN####

import pyspark.sql.functions as f
from pyspark.sql.functions import lower, col, lit, when

# make InsName column of df all lowercase

dff1 = Step2.withColumn("FilteredInsName", lower(col("InsName")))
#display(dff1.select("InsName","FilteredInsName"))

#remove all _company
dff2int = dff1.withColumn("Ins_company", f.regexp_replace('FilteredInsName', ' company', ''))
dff2int = dff2int.drop("FilteredInsName")
dff2int = dff2int.withColumnRenamed("Ins_company","FilteredInsName")
#display(dff2int.select("InsName","FilteredInsName"))

# remove substrings: .com 
dff2 = dff2int.withColumn("InsCom", f.regexp_replace('FilteredInsName', '.com', ''))
dff2 = dff2.drop("FilteredInsName")
dff2 = dff2.withColumnRenamed("InsCom","FilteredInsName")
#display(dff2.select("InsName","FilteredInsName"))

# remove substrings: .net 
dff3int = dff2.withColumn("InsNet", f.regexp_replace('FilteredInsName', '.net', ''))
dff3 = dff3int.drop("FilteredInsName")
dff3 = dff3.withColumnRenamed("InsNet","FilteredInsName")
#display(dff3.select("InsName","FilteredInsName"))

# remove substrings: .org
dff4int = dff3.withColumn("InsOrg", f.regexp_replace('FilteredInsName', '.org', ''))
dff4 = dff4int.drop("FilteredInsName")
dff4 = dff4.withColumnRenamed("InsOrg","FilteredInsName")
#display(dff4.select("InsName","FilteredInsName"))

####RUN 6
# remove common phrases            ####RUN####

#remove all _corporation
dff5indf = dff4.withColumn("Ins_corporation", f.regexp_replace('FilteredInsName', ' corporation', ''))
dff5int = dff5indf.drop("FilteredInsName")
dff5int = dff5int.withColumnRenamed("Ins_corporation","FilteredInsName")
#display(dff7int.select("InsName","FilteredInsName"))
#remove all _corp
dff6indf = dff5int.withColumn("Ins_corp", f.regexp_replace('FilteredInsName', ' corp', ''))
dff6int = dff6indf.drop("FilteredInsName")
dff6int = dff6int.withColumnRenamed("Ins_corp","FilteredInsName")
#display(dff6int.select("InsName","FilteredInsName"))
# remove all _co.
dff7indf = dff6int.withColumn("Ins_co", f.regexp_replace('FilteredInsName', ' co.', ''))
dff7int = dff7indf.drop("FilteredInsName")
dff7int = dff7int.withColumnRenamed("Ins_co","FilteredInsName")
#display(dff5int.select("InsName","FilteredInsName"))

#remove all _incorporated
dff8indf = dff7int.withColumn("Ins_incorporated", f.regexp_replace('FilteredInsName', ' incorporated', ''))
dff8int = dff8indf.drop("FilteredInsName")
dff8int = dff8int.withColumnRenamed("Ins_incorporated","FilteredInsName")
#display(dff8int.select("InsName","FilteredInsName"))
#remove all _inc
dff9indf = dff8int.withColumn("Ins_inc", f.regexp_replace('FilteredInsName', ' inc', ''))
dff9int = dff9indf.drop("FilteredInsName")
dff9int = dff9int.withColumnRenamed("Ins_inc","FilteredInsName")
#display(dff9int.select("InsName","FilteredInsName"))

#remove all _llc
dff10indf = dff9int.withColumn("Ins_llc", f.regexp_replace('FilteredInsName', ' llc', ''))
dff10int = dff10indf.drop("FilteredInsName")
dff10int = dff10int.withColumnRenamed("Ins_llc","FilteredInsName")
#display(dff10int.select("InsName","FilteredInsName"))

#remove all _ltd
dff11indf = dff10int.withColumn("Ins_ltd", f.regexp_replace('FilteredInsName', ' ltd', ''))
dff11int = dff11indf.drop("FilteredInsName")
dff11int = dff11int.withColumnRenamed("Ins_ltd","FilteredInsName")
#display(dff11int.select("InsName","FilteredInsName"))
# remove all _limited
dff12indf = dff11int.withColumn("Ins_limited", f.regexp_replace('FilteredInsName', ' limited', ''))
dff12int = dff12indf.drop("FilteredInsName")
dff12int = dff12int.withColumnRenamed("Ins_limited","FilteredInsName")
#display(dff12int.select("InsName","FilteredInsName"))

#remove all _lp
dff13indf = dff12int.withColumn("Ins_lp", f.regexp_replace('FilteredInsName', ' lp', ''))
dff13int = dff13indf.drop("FilteredInsName")
dff13int = dff13int.withColumnRenamed("Ins_lp","FilteredInsName")
#display(dff13int.select("InsName","FilteredInsName"))
#remove all _l.p
dff14indf = dff13int.withColumn("Ins_l*p", f.regexp_replace('FilteredInsName', ' l.p', ''))
dff14int = dff14indf.drop("FilteredInsName")
dff14int = dff14int.withColumnRenamed("Ins_l*p","FilteredInsName")
#display(dff14int.select("InsName","FilteredInsName"))

#remove all _llp
dff15indf = dff14int.withColumn("Ins_llp", f.regexp_replace('FilteredInsName',' llp',''))
dff15int = dff15indf.drop("FilteredInsName")
dff15int = dff15int.withColumnRenamed("Ins_llp","FilteredInsName")
#display(dff15int.select("InsName","FilteredInsName"))

#remove special including spaces
dff16indf = dff15int.withColumn("InsSpecial", f.translate(f.col("FilteredInsName"), "!@#$%^&*()_+/<>:;'.,'\" ", "").alias("replaced"))
dff16int = dff16indf.drop("FilteredInsName")
dff16int = dff16int.withColumnRenamed("InsSpecial","dfFilteredInsName")
#display(dff16int.select("InsName","dfFilteredInsName"))

##################### db STRING PROCESSING ##############################

####RUN 7
## Remove Web suffixes                 ####RUN####

import pyspark.sql.functions as f
from pyspark.sql.functions import lower, col, lit, when

# make InsName column of df all lowercase
db1f1 = db.withColumn("FilteredInsName", lower(col("BusinessName")))
#display(db1f1.select("BusinessName","FilteredInsName"))

#remove all _company
db1f2int = db1f1.withColumn("Ins_company", f.regexp_replace('FilteredInsName', ' company', ''))
db1f2int = db1f2int.drop("FilteredInsName")
db1f2int = db1f2int.withColumnRenamed("Ins_company","FilteredInsName")
#display(db1f2int.select("BusinessName","FilteredInsName"))

# remove substrings: .com 
db1f2 = db1f2int.withColumn("InsCom", f.regexp_replace('FilteredInsName', '.com', ''))
db1f2 = db1f2.drop("FilteredInsName")
db1f2 = db1f2.withColumnRenamed("InsCom","FilteredInsName")
#display(db1f2.select("BusinessName","FilteredInsName"))

# remove substrings: .net 
db1f3int = db1f2.withColumn("InsNet", f.regexp_replace('FilteredInsName', '.net', ''))
db1f3 = db1f3int.drop("FilteredInsName")
db1f3 = db1f3.withColumnRenamed("InsNet","FilteredInsName")
#display(db1f3.select("BusinessName","FilteredInsName"))

# remove substrings: .org
db1f4int = db1f3.withColumn("InsOrg", f.regexp_replace('FilteredInsName', '.org', ''))
db1f4 = db1f4int.drop("FilteredInsName")
db1f4 = db1f4.withColumnRenamed("InsOrg","FilteredInsName")
#display(db1f4.select("BusinessName","FilteredInsName"))

####RUN 8
# remove common phrases                 ####RUN####

#remove all _corporation
db1f5indf = db1f4.withColumn("Ins_corporation", f.regexp_replace('FilteredInsName', ' corporation', ''))
db1f5int = db1f5indf.drop("FilteredInsName")
db1f5int = db1f5int.withColumnRenamed("Ins_corporation","FilteredInsName")
#display(db1f7int.select("BusinessName","FilteredInsName"))
#remove all _corp
db1f6indf = db1f5int.withColumn("Ins_corp", f.regexp_replace('FilteredInsName', ' corp', ''))
db1f6int = db1f6indf.drop("FilteredInsName")
db1f6int = db1f6int.withColumnRenamed("Ins_corp","FilteredInsName")
#display(db1f6int.select("BusinessName","FilteredInsName"))
# remove all _co.
db1f7indf = db1f6int.withColumn("Ins_co", f.regexp_replace('FilteredInsName', ' co.', ''))
db1f7int = db1f7indf.drop("FilteredInsName")
db1f7int = db1f7int.withColumnRenamed("Ins_co","FilteredInsName")
#display(db1f5int.select("BusinessName","FilteredInsName"))

#remove all _incorporated
db1f8indf = db1f7int.withColumn("Ins_incorporated", f.regexp_replace('FilteredInsName', ' incorporated', ''))
db1f8int = db1f8indf.drop("FilteredInsName")
db1f8int = db1f8int.withColumnRenamed("Ins_incorporated","FilteredInsName")
#display(db1f8int.select("BusinessName","FilteredInsName"))
#remove all _inc
db1f9indf = db1f8int.withColumn("Ins_inc", f.regexp_replace('FilteredInsName', ' inc', ''))
db1f9int = db1f9indf.drop("FilteredInsName")
db1f9int = db1f9int.withColumnRenamed("Ins_inc","FilteredInsName")
#display(db1f9int.select("BusinessName","FilteredInsName"))

#remove all _llc
db1f10indf = db1f9int.withColumn("Ins_llc", f.regexp_replace('FilteredInsName', ' llc', ''))
db1f10int = db1f10indf.drop("FilteredInsName")
db1f10int = db1f10int.withColumnRenamed("Ins_llc","FilteredInsName")
#display(db1f10int.select("BusinessName","FilteredInsName"))

#remove all _ltd
db1f11indf = db1f10int.withColumn("Ins_ltd", f.regexp_replace('FilteredInsName', ' ltd', ''))
db1f11int = db1f11indf.drop("FilteredInsName")
db1f11int = db1f11int.withColumnRenamed("Ins_ltd","FilteredInsName")
#display(db1f11int.select("BusinessName","FilteredInsName"))
# remove all _limited
db1f12indf = db1f11int.withColumn("Ins_limited", f.regexp_replace('FilteredInsName', ' limited', ''))
db1f12int = db1f12indf.drop("FilteredInsName")
db1f12int = db1f12int.withColumnRenamed("Ins_limited","FilteredInsName")
#display(db1f12int.select("BusinessName","FilteredInsName"))

#remove all _lp
db1f13indf = db1f12int.withColumn("Ins_lp", f.regexp_replace('FilteredInsName', ' lp', ''))
db1f13int = db1f13indf.drop("FilteredInsName")
db1f13int = db1f13int.withColumnRenamed("Ins_lp","FilteredInsName")
#display(db1f13int.select("BusinessName","FilteredInsName"))
#remove all _l.p
db1f14indf = db1f13int.withColumn("Ins_l*p", f.regexp_replace('FilteredInsName', ' l.p', ''))
db1f14int = db1f14indf.drop("FilteredInsName")
db1f14int = db1f14int.withColumnRenamed("Ins_l*p","FilteredInsName")
#display(db1f14int.select("BusinessName","FilteredInsName"))

#remove all _llp
db1f15indf = db1f14int.withColumn("Ins_llp", f.regexp_replace('FilteredInsName',' llp',''))
db1f15int = db1f15indf.drop("FilteredInsName")
db1f15int = db1f15int.withColumnRenamed("Ins_llp","FilteredInsName")
#display(db1f15int.select("BusinessName","FilteredInsName"))

#remove special including spaces
db1f16indf = db1f15int.withColumn("InsSpecial", f.translate(f.col("FilteredInsName"), "!@#$%^&*()_+/<>:;'.,'\" ", "").alias("replaced"))
db1f16int = db1f16indf.drop("FilteredInsName")
db1f16int = db1f16int.withColumnRenamed("InsSpecial","dbFilteredInsName")
#display(db1f16int.select("BusinessName","dbFilteredInsName"))

##################### join db with df ##############################
####RUN 9
# rename filtered tables  ####RUN####

db_filter = db1f16int
db_filter= db_filter.withColumnRenamed("dbFilteredInsName","FilteredInsName")
db_filter = db_filter.select("Revenue","BusinessName","FilteredInsName","SIC1","NAICS1Code","EmployeesTotal")
db_filter = db_filter.withColumnRenamed("SIC1","SICCode")
db_filter = db_filter.withColumnRenamed("NAICS1Code","NAICSCode")

df_filter = dff16int
df_filter= df_filter.withColumnRenamed("dfFilteredInsName","FilteredInsName")
df_filter = df_filter.select("Pol_Num","FilteredInsName","InsName","State","City","ZIP_Code")

# grand_finale: master table where db is joined with df
grand_finale= df_filter.join(db_filter, ["FilteredInsName"])


#Run
InsNameRev = grand_finale.select("BusinessName","InsName","Revenue","SICCode","NAICSCode","EmployeesTotal").distinct()

#Run###
Step3= Step2.join(InsNameRev, ["InsName"])

### Extract results from Method #2

### Run this line to select highest revenue

e = Step3.filter(Step3['Revenue'] > 0)


# DBTITLE 1,Layer_2 Output
### Layer_2 groupBy Max Revenue
### GROUP BY MAX REVENUE

from pyspark.sql import Window
w = Window.partitionBy('InsName')
Layer_2 = e.withColumn('maxRevenue', f.max('Revenue').over(w))\
    .where(f.col('Revenue') == f.col('maxRevenue'))\
    .drop('maxRevenue')
Layer_2 = Layer_2.select("Pol_Num","BusinessName","InsName","Revenue","SICCode","NAICSCode","EmployeesTotal","State","City","ZIP_Code")

display(Layer_2)
