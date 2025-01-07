from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark SQL basic example").enableHiveSupport().getOrCreate()

sc = spark.sparkContext


file_path = 'file:///home/talentum/test-jupyter/test/PairRdd/flights.csv'

df = spark.read.csv(file_path)
df.show(5)

carrier_counts = df.select(df._c5)\
    .groupBy('_c5')\
    .agg(F.count('*').alias('count'))\
    .orderBy(F.desc('count'))

carrier_counts.show(3)

#### 2. Determine the most common routes between two cities

airports_path = 'file:///home/talentum/test-jupyter/test/PairRdd/airports.csv'

airports_df = spark.read.csv(airports_path, header=True)
airports_df.show()

city_df = airports_df.select(
    F.col("iata").alias("iata"),
    F.col("city").alias("city")
)
city_df.show(5)

flights_path = 'file:///home/talentum/test-jupyter/test/PairRdd/flights.csv'

flights_df = spark.read.csv(flights_path)

flight_orig_dest_df = flights_df.select(
    F.col("_c12").alias("origin"),
    F.col("_c13").alias("destination")
)
flight_orig_dest_df.show(5)


origin_joined_df = flight_orig_dest_df.join(
    F.broadcast(city_df),
    flight_orig_dest_df.origin == city_df.iata,
    "inner"
).select("destination", "city").withColumnRenamed("city", "origin_city")


dest_origin_joined_df = origin_joined_df.join(
    F.broadcast(city_df),
    origin_joined_df.destination == city_df.iata,
    "inner"
).select("origin_city", "city").withColumnRenamed("city", "dest_city")

city_pairs_count = dest_origin_joined_df\
    .groupBy("origin_city", "dest_city")\
    .agg(F.count("*").alias("count"))\
    .orderBy(F.desc("count"))

city_pairs_count.show(5)

#### 3. Find the longest departure delays (15 min or more)

flights_df = spark.read.csv('file:///home/talentum/test-jupyter/test/PairRdd/flights.csv')

delays_df = flights_df.select(
    F.col("_c5").alias("flight_num"),
    F.col("_c11").cast(IntegerType()).alias("delay")
).filter(F.col("delay") > 15)
print("Delays DataFrame:")
delays_df.show(5)

max_delays_df = delays_df.groupBy("flight_num").agg(F.max("delay").alias("max_delay")).orderBy("max_delay", ascending=False)
print("Maximum Delays DataFrame:")
max_delays_df.show(5)

#### 4. Remove the records that contain incomplete details

file_path = 'file:///home/talentum/test-jupyter/test/PairRdd/plane-data.csv'

plane_data_df = spark.read.csv(
    file_path,
    header=True,
    inferSchema=True
)
print("Total plane records:", plane_data_df.count())

filtered_df = plane_data_df.filter("type IS NOT NULL")

print(filtered_df.count())