from pyspark.sql import SparkSession
from pyspark.context import SparkContext

def session_list(x):
    lis=[1]
    session=1
    i=1
    k=0
    while(i<len(x)):
        if (int(x[i][11:13])*3600+int(x[i][14:16])*60+int(x[i][17:19])) - (int(x[k][11:13])*3600+int(x[k][14:16])*60+int(x[k][17:19]))<1800:
            lis.append(session)
            i+=1
        else:
            session+=1
            k=i
            i+=1
            lis.append(session)
    return lis 

sc = SparkContext.getOrCreate()
spark = SparkSession.builder.appName("").config("spark.dynamicAllocation.enabled", True) \
    .config("spark.executor.cores", 2) \
    .config("spark.dynamicAllocation.minExecutors", 2) \
    .config("spark.dynamicAllocation.maxExecutors", 7) \
    .config("spark.dynamicAllocation.initialExecutors", 2) \
    .config("spark.ui.port", 4095).getOrCreate()
df=spark.read.parquet("s3://connecto-models/_temp/vdp_views/data/2022/07/01/")


import pyspark.sql.functions as F
df1=df.groupBy("anonymousId").agg(F.collect_list("used_carid"),F.collect_list("timestamp"))
df2=df1.withColumnRenamed("collect_list(used_carid)","  Used_carId").withColumnRenamed("collect_list(timestamp)","time")
print(df2.show(5))


from pyspark.sql.functions import sort_array
from pyspark.sql.functions import *
sorted_list=df2.select("*",sort_array(df2.time)).withColumnRenamed("sort_array(time, true)","sorted_time")


from pyspark.sql.functions import explode
from pyspark.sql.types import IntegerType 
user_defined = udf(lambda z:session_list(z),StringType())   
df3=sorted_list.withColumn("session_no", user_defined(sorted_list.sorted_time))
df4=df3.withColumn("session_no",split(col("session_no"),",").cast("array<long>"))
df5=df4.withColumn("session_no",explode(col("session_no")))
print(df5.count()-df.count())


#Results
df6=df5.select("anonymousId","session_no")
final_list=df6.groupBy("anonymousId","session_no").count().withColumnRenamed("count","VDP_views")
print(final_list.show(10))
print(final_list.describe().show())
users_more_than=final_list.filter(final_list.VDP_views>1).count()
print(users_more_than)


