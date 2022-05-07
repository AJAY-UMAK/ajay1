import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window


spark = SparkSession.builder.master("local[*]") \
                    .appName("Airline") \
                    .getOrCreate()
columns ='airline_id int,Name string,Alias string,IATA string,ICAO string,Callsign string,Country string,Active string'
airline_df = spark.read.csv(r'C:\\BigDataLocalSetup\\Spark\\bin\\Airline Project\\airline.csv',schema =columns)
airline_df.createOrReplaceTempView('airline_df')
#airline_df.show()

columns2 = 'airport_id int,Name string,City string,Country string,IATA string,ICAO string,Latitude float,Longitude float,Altitude float,Timezone float,DST string,Tz string,Type string,Source string'
airport_df = spark.read.csv(r'C:\\BigDataLocalSetup\\Spark\\bin\\Airline Project\\airport.csv',schema = columns2)
airport_df.createOrReplaceTempView("airport_df")
#airport_df.show()

columns3 ='Name string,IATA string,ICAO string'
plane_df = spark.read.csv(r"C:\\BigDataLocalSetup\\Spark\\bin\\Airline Project\\plane.csv",schema = columns3,header = True,sep='SDH')
plane_df.createOrReplaceTempView("plane_df")
#plane_df.show()

route_df = spark.read.parquet(r"C:\\BigDataLocalSetup\\Spark\\bin\\Airline Project\\routes.snappy.parquet")
route_df.createOrReplaceTempView("route_df")
#route_df.show()

# 1. In any of your input file if you are getting \N or null values in your column and that column is of string type then put default value as "(unknown)" and if column is of type integer then put -1

airline_df = airline_df.withColumn("Alias", expr("CASE WHEN Alias IS NULL THEN 'UNKNOWN' ELSE Alias END")) \
                       .withColumn("IATA", expr("CASE WHEN IATA IS NULL THEN 'UNKNOWN' ELSE IATA END")) \
                       .withColumn("ICAO", expr("CASE WHEN ICAO IS NULL THEN 'UNKNOWN' ELSE ICAO END")) \
                       .withColumn("Callsign", expr("CASE WHEN Callsign IS NULL THEN 'UNKNOWN' ELSE Callsign END")) \
                       .withColumn("Country", expr("CASE WHEN Country IS NULL THEN 'UNKNOWN' ELSE Country END")) \
                       .withColumn("Active", expr("CASE WHEN Active IS NULL THEN 'UNKNOWN' ELSE Active END")) \

airline_df = airline_df.replace(["\\N"],["UNKNOWN"],["Alias","IATA","ICAO","Callsign","Country","Active"])
#airline_df.show()

# 2. find the country name which is having both airlines and airport
#DataFrame:

#airline_df.select('country').union(airport_df.select('country')).distinct().show()

#Sql:

#spark.sql('select b.country from airline_df a, airport_df b where a.country = b.country').distinct().show()

#3. get the airlines details like name, id,. which is has taken takeoff more than 3 times from same airport
#DataFrame:

#airline_df.join(route_df, on='airline_id',how='inner').groupBy('src_airport',airline_df.airline_id,airline_df.Name).count().filter(col('count')>3).show()

#Sql:

#spark.sql("select a.airline_id, a.name, r.src_airport,count(*) from airline_df a join route_df r on(a.airline_id=r.airline_id) group by a.airline_id, a.name, r.src_airport having count(*)>3").show()

#4. get airport details which has minimum number of takeoffs and landing.
#DataFrame:
a = route_df.select('*').groupBy('src_airport').count().orderBy('count')
#a.show()

b = route_df.select('*').groupBy('dest_airport').count().orderBy('count')
#b.show()

c=a.union(b).groupBy('src_airport').sum('count').filter(col('sum(count)')==1).select('src_airport')

airport_df.join(c,airport_df.IATA==c.src_airport,'inner')
#c.show()

#Sql:
spark.sql("select * from airport_df c where c.IATA in ( select b.src_airport from  ( select a.src_airport,sum(shri),dense_rank() over(order by sum(shri) asc) as dr from ( select src_airport , count(*) as shri from route_df group by src_airport  union select dest_airport,count(*)  from route_df group by dest_airport ) a group by a.src_airport order by 2 asc ) b where dr=1)").show()

# 5.get airport details which has maximum number of takeoffs and landing.
a = route_df.select('*').groupBy('src_airport').count().orderBy('count',ascending=False)
#a.show()

b = route_df.select('*').groupBy('dest_airport').count().orderBy('count',ascending=False)
#b.show()

#a.union(b).groupBy('src_airport').sum('count').orderBy('sum(count)',ascending=False).agg(sum('sum(count)')).show()

#c = a.union(b).groupBy('src_airport').sum('count').orderBy('sum(count)',ascending=False).withColumn('rank',rank().over(Window.orderBy(col('sum(count)').desc()))).filter(col('rank')==1).select('src_airport')

#airport_df.join(c,airport_df.IATA==c.src_airport,'inner').show()

#Sql:
#spark.sql("select a.src_airport,sum(shri) from ( select src_airport , count(*) as shri from route_df group by src_airport union select dest_airport,count(*)  from route_df group by dest_airport ) a group by a.src_airport order by 2 desc ").show()
#spark.sql("select * from airport_df c where c.IATA= ( select b.src_airport from (select a.src_airport,sum(shri),row_number() over(order by sum(shri) desc) as rownum from ( select src_airport , count(*) as shri from route_df group by src_airport union select dest_airport,count(*) from route_df group by dest_airport) a group by a.src_airport order by 2 desc )b where rownum=1 )").show()

# 6.Get the airline details, which is having direct flights. details like airline id, name, source airport name, and destination airport name

# dataframe :

#route_df.join(airline_df,on='airline_id').select('*').filter(route_df.stops=='0').select('airline_id','name','src_airport','dest_airport').show()

# SparkSQL :

spark.sql("select airline_df.airline_id,name,src_airport,dest_airport from airline_df join route_df on (airline_df.airline_id=route_df.airline_id) where route_df.stops='0' ").show()