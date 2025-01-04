## PySpark Notes

### Introduction to PySpark
PySpark is the Python API for Apache Spark, an open-source distributed computing system that provides a fast and general-purpose cluster-computing framework for big data processing. PySpark allows you to harness the simplicity of Python while leveraging Spark's power for handling large datasets.

---

### Key Features of PySpark
1. **In-Memory Computation**: Fast data processing by keeping data in memory.
2. **Fault Tolerance**: Automatically recovers from node failures.
3. **Distributed Processing**: Scales across multiple nodes for parallel data processing.
4. **Support for Multiple Languages**: APIs available in Python, Scala, Java, and R.
5. **Built-in Libraries**: Includes MLlib for machine learning, Spark SQL for querying, GraphX for graph processing, and more.

---

### Core Components

#### 1. **RDD (Resilient Distributed Dataset)**
RDD is the fundamental data structure in Spark. It is immutable, distributed, and fault-tolerant.

##### Key Operations on RDD:
- **Actions:**
    - `collect()`
    - `count()`
    - `take(n)`
    - `first()`
    - `reduce(func)`
    - `countByKey()`
    - `saveAsTextFile(path)`
    - `saveAsSequenceFile(path)`
    - `saveAsHadoopFile(path)`
    - `takeOrdered(n, key=None)`
    - `foreach(func)`
    - `lookup(key)`
    - `countByValue()`
    - `top(n)`

- **Transformations:**
    - `map(func)`
    - `filter(func)`
    - `flatMap(func)`
    - `reduceByKey(func)`
    - `groupByKey()`
    - `sortByKey(ascending=True, numPartitions=None)`
    - `join(other)`
    - `leftOuterJoin(other)`
    - `rightOuterJoin(other)`
    - `union(other)`
    - `intersection(other)`
    - `distinct()`
    - `coalesce(numPartitions)`
    - `repartition(numPartitions)`
    - `partitionBy(numPartitions, partitionFunc)`
    - `cartesian(other)`
    - `sample(withReplacement, fraction, seed=None)`

- **Others:**
    - `cache()`
    - `persist(storageLevel)`
    - `unpersist()`
          
##### Example:
```python
from pyspark import SparkContext

# Create a SparkContext object for local execution
sc = SparkContext("local", "example")

# Sample data: A simple list of integers
data = [1, 2, 3, 4, 5]

# Create an RDD from the data
rdd = sc.parallelize(data)

# Transformation: Apply a map to square each element
squared_rdd = rdd.map(lambda x: x ** 2)

# Action: Collect the results of the transformation
squared = squared_rdd.collect()
print(squared)  # Output: [1, 4, 9, 16, 25]

# Transformation: Filter the original RDD to keep only even numbers
filtered_rdd = rdd.filter(lambda x: x % 2 == 0)

# Action: Collect the filtered elements
filtered = filtered_rdd.collect()
print(filtered)  # Output: [2, 4]

# Transformation: Apply a flatMap to generate a list of tuples
flat_mapped_rdd = rdd.flatMap(lambda x: [(x, x**2)])

# Action: Collect the flattened result
flat_mapped = flat_mapped_rdd.collect()
print(flat_mapped)  # Output: [(1, 1), (2, 4), (3, 9), (4, 16), (5, 25)]

# Action: Count the number of elements in the RDD
count = rdd.count()
print(count)  # Output: 5

# Action: Reduce the elements of the RDD by summing them
sum_result = rdd.reduce(lambda x, y: x + y)
print(sum_result)  # Output: 15

# Stop the SparkContext to release resources
sc.stop()

```

#### 2. **DataFrame**
DataFrames are distributed collections of data organized into named columns, similar to tables in a database.

##### Features:
- High-level API for structured data.
- Supports SQL queries.
- Optimized via Catalyst engine.

##### Key Operations on DataFrame:
- **Actions:**
    - `show(n=20, truncate=True)`
    - `count()`
    - `collect()`
    - `head(n)`
    - `take(n)`
    - `toPandas()`
    - `write.format(name).save(path)`
    - `write.json(path)`
    - `write.csv(path)`
    - `write.parquet(path)`
    - `write.orc(path)`
    - `describe(*cols)`
    - `summary()`

- **Transformations:**
    - `select(*cols)`
    - `filter(condition)`
    - `groupBy(*cols)`
    - `agg(*exprs)`
    - `join(other, on=None, how=None)`
    - `withColumn(name, col)`
    - `drop(*cols)`
    - `distinct()`
    - `orderBy(*cols, ascending=True)`
    - `limit(n)`
    - `repartition(numPartitions, *cols)`
    - `coalesce(numPartitions)`
    - `alias(aliasName)`
    - `fillna(value, subset=None)`
    - `replace(to_replace, value, subset=None)`
    - `dropna(how, thresh, subset=None)`
    - `sample(withReplacement, fraction, seed=None)`
    - `explode(column)`

- **Others:**
    - `cache()`
    - `persist(storageLevel)`
    - `unpersist()`
    - `isEmpty()`

##### Example:
```python
from pyspark.sql import SparkSession

# Initialize a SparkSession to work with DataFrames
spark = SparkSession.builder.appName("example").getOrCreate()

# Sample data: A list of tuples representing (Name, Age)
data = [("Alice", 29), ("Bob", 35), ("Catherine", 25)]
columns = ["Name", "Age"]

# Create a DataFrame from the data and columns
df = spark.createDataFrame(data, columns)

# Display the DataFrame (first 20 rows by default)
df.show()

# Output:
# +---------+---+
# |     Name|Age|
# +---------+---+
# |    Alice| 29|
# |      Bob| 35|
# |Catherine| 25|
# +---------+---+

# Select a specific column ("Name") and display it
df.select("Name").show()

# Output:
# +---------+
# |     Name|
# +---------+
# |    Alice|
# |      Bob|
# |Catherine|
# +---------+

# Filter rows where Age is greater than 30 and display the result
df.filter(df.Age > 30).show()

# Output:
# +---+---+
# |Name|Age|
# +---+---+
# | Bob| 35|
# +---+---+

# Add a new column "Age_10_years" by adding 10 to the "Age" column
updated_df = df.withColumn("Age_10_years", df.Age + 10)

# Display the updated DataFrame with the new column
updated_df.show()

# Output:
# +---------+---+------------+
# |     Name|Age|Age_10_years|
# +---------+---+------------+
# |    Alice| 29|          39|
# |      Bob| 35|          45|
# |Catherine| 25|          35|
# +---------+---+------------+

# Perform SQL queries (Spark SQL needs to register the DataFrame as a temporary view first)
df.createOrReplaceTempView("people")

# Run an SQL query to select names and ages of people older than 30
sql_result = spark.sql("SELECT Name, Age FROM people WHERE Age > 30")
sql_result.show()

# Output:
# +---+---+
# |Name|Age|
# +---+---+
# | Bob| 35|
# +---+---+

# Stop the SparkSession to release resources
spark.stop()
```

#### 3. **Spark SQL**
Spark SQL allows querying structured data using SQL syntax.



##### Key Operations on SQL:
- **Actions:**
    - `sql(query)`
    - `table(tableName)`
    - `createOrReplaceTempView(viewName)`
    - `createGlobalTempView(viewName)`

- **Transformations:**
    - Primarily used for querying with SQL syntax (e.g., SELECT, JOIN, GROUP BY).

- **Others:**
    - Integration with Hive (`enableHiveSupport()`, `setConf(key, value)`).


##### Example:
```python
from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.appName("spark_sql_example").getOrCreate()

# Sample data: A list of tuples representing (Name, Age)
data = [("Alice", 29), ("Bob", 35), ("Catherine", 25), ("David", 40)]
columns = ["Name", "Age"]

# Create a DataFrame from the sample data
df = spark.createDataFrame(data, columns)

# Register the DataFrame as a temporary SQL view to run SQL queries
df.createOrReplaceTempView("people")

# Use Spark SQL to query the 'people' view and select Name, Age where Age is greater than 30
result = spark.sql("SELECT Name, Age FROM people WHERE Age > 30")

# Display the query result
result.show()

# Output:
# +-----+---+
# | Name|Age|
# +-----+---+
# |  Bob| 35|
# |David| 40|
# +-----+---+

# Perform another SQL query: Find the average age of people
avg_age = spark.sql("SELECT AVG(Age) AS avg_age FROM people")
avg_age.show()

# Output:
# +-------+
# |avg_age|
# +-------+
# |   32.25|
# +-------+

# Perform a more complex SQL query: Count people by age groups
age_group_count = spark.sql("""
    SELECT
        CASE 
            WHEN Age < 30 THEN 'Under 30'
            WHEN Age >= 30 AND Age <= 40 THEN '30-40'
            ELSE 'Above 40'
        END AS AgeGroup,
        COUNT(*) AS Count
    FROM people
    GROUP BY
        CASE 
            WHEN Age < 30 THEN 'Under 30'
            WHEN Age >= 30 AND Age <= 40 THEN '30-40'
            ELSE 'Above 40'
        END
""")
age_group_count.show()

# Output:
# +-------+-----+
# |AgeGroup|Count|
# +-------+-----+
# |  Under 30|    2|
# |    30-40|    2|
# +-------+-----+

# Stop the SparkSession to release resources
spark.stop()

```

#### 4. **MLlib**
Spark’s machine learning library provides tools for:
- Classification
- Regression
- Clustering
- Recommendation systems

##### Key Operations on MLlib:
- **Actions:**
    - `fit(dataset)`
    - `transform(dataset)`
    - `save(path)`
    - `load(path)`
    - `evaluate(dataset)`

- **Transformations:**
    - `setParams(**params)`
    - `setFeaturesCol(value)`
    - `setLabelCol(value)`
    - `setPredictionCol(value)`
    - `setMaxIter(value)`
    - `setRegParam(value)`
    - `setThreshold(value)`
    - `setStepSize(value)`

##### Example:
```python
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# Initialize a SparkSession
spark = SparkSession.builder.appName("MLlib_example").getOrCreate()

# Sample data: Tuples with label and features for regression
data = [(1.0, 2.0, 3.0, 4.0), (5.0, 6.0, 7.0, 8.0), (9.0, 10.0, 11.0, 12.0), (13.0, 14.0, 15.0, 16.0)]
columns = ["label", "feature1", "feature2", "feature3"]

# Create DataFrame from the sample data
df = spark.createDataFrame(data, columns)

# Show the data
df.show()

# Output:
# +-----+--------+--------+--------+
# |label|feature1|feature2|feature3|
# +-----+--------+--------+--------+
# |  1.0|     2.0|     3.0|     4.0|
# |  5.0|     6.0|     7.0|     8.0|
# |  9.0|    10.0|    11.0|    12.0|
# | 13.0|    14.0|    15.0|    16.0|
# +-----+--------+--------+--------+

# Prepare features for training by combining feature columns into a single vector column
assembler = VectorAssembler(inputCols=["feature1", "feature2", "feature3"], outputCol="features")
data = assembler.transform(df)

# Show the transformed data with the 'features' column
data.select("label", "features").show()

# Output:
# +-----+-------------+
# |label|     features|
# +-----+-------------+
# |  1.0| [2.0,3.0,4.0]|
# |  5.0| [6.0,7.0,8.0]|
# |  9.0|[10.0,11.0,12.0]|
# | 13.0|[14.0,15.0,16.0]|
# +-----+-------------+

# Initialize a Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="label")

# Train the Linear Regression model using the prepared data
model = lr.fit(data)

# Display the model's coefficients and intercept
print("Coefficients: ", model.coefficients)
print("Intercept: ", model.intercept)

# Make predictions on the training data
predictions = model.transform(data)

# Show the predictions
predictions.select("label", "prediction").show()

# Output:
# +-----+----------+
# |label|prediction|
# +-----+----------+
# |  1.0|   1.00000|
# |  5.0|   5.00000|
# |  9.0|   9.00000|
# | 13.0|  13.00000|
# +-----+----------+

# Show detailed metrics from the model's summary
training_summary = model.summary
print("Root Mean Squared Error (RMSE):", training_summary.rootMeanSquaredError)
print("R2:", training_summary.r2)

# Stop the SparkSession to release resources
spark.stop()
```

#### 5. **Streaming**
Spark Streaming is used for processing real-time data streams.

##### Key Operations on Streaming:
- **Actions:**
    - `start()`
    - `awaitTermination()`
    - `stop()`
    - `isActive()`

- **Transformations:**
    - `map(func)`
    - `flatMap(func)`
    - `filter(func)`
    - `reduceByKeyAndWindow(func, windowDuration, slideDuration)`
    - `updateStateByKey(func)`
    - `window(windowDuration, slideDuration)`
    - `join(otherStream)`
    - `union(otherStream)`
    - `transform(func)`
	- `countByWindow(windowDuration, slideDuration)`
	- `countByValueAndWindow(windowDuration, slideDuration)`

- **Others:**
    - `checkpoint(directory)`
    - `remember(duration)`

##### Example:
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

# Initialize a SparkSession for Structured Streaming
spark = SparkSession.builder.appName("streaming_example").getOrCreate()

# Read streaming data from a socket on localhost:9999
# The format "socket" is used to read data coming from a network socket
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Show the schema of the input data (streamed lines)
lines.printSchema()

# Output:
# root
#  |-- value: string (nullable = true)

# Split each line of text into words using space as the delimiter
# 'explode' will transform each row into multiple rows for each word
words = lines.select(explode(split(lines.value, " ")).alias("word"))

# Group the words by their occurrences and count the occurrences of each word
word_counts = words.groupBy("word").count()

# Write the output stream to the console in "complete" output mode
# "complete" mode is used when we want the full result of the aggregation (all words and counts) to be output in each batch
query = word_counts.writeStream.outputMode("complete").format("console").start()

# Wait for the streaming query to terminate (block the program until the stream ends)
query.awaitTermination()

# Note: Press Ctrl+C to stop the streaming application.

```

---

### Common Operations

---

### PySpark SQL Functions
PySpark provides a rich set of built-in functions for working with data. Here are some of the most commonly used ones:

#### String Functions
- `concat(*cols)`: Concatenates multiple columns.
- `instr(col, substring)`: Returns the position of the first occurrence of the substring in the column.
- `lower(col)`, `upper(col)`: Converts strings to lowercase or uppercase.
- `length(col)`: Returns the length of the string.
- `trim(col)`, `ltrim(col)`, `rtrim(col)`: Trims spaces from strings.

#### Date and Time Functions
- `current_date()`, `current_timestamp()`: Returns the current date and timestamp.
- `date_add(start, days)`, `date_sub(start, days)`: Adds or subtracts days from a date.
- `datediff(end, start)`: Returns the difference in days between two dates.
- `year(col)`, `month(col)`, `dayofmonth(col)`: Extracts year, month, or day from a date.

#### Aggregation Functions
- `count(col)`, `sum(col)`, `avg(col)`, `max(col)`, `min(col)`: Basic aggregate functions.
- `collect_list(col)`, `collect_set(col)`: Returns all or unique values as a list.

#### Conditional Functions
- `when(condition, value).otherwise(default)`: Conditional logic.
- `if(col, valueTrue, valueFalse)`: Inline conditional expressions.

#### Miscellaneous Functions
- `lit(value)`: Creates a column with a constant value.
- `col(name)`: Refers to a column by name.
- `explode(col)`: Expands arrays or maps into separate rows.
- `split(col, pattern)`: Splits strings into arrays based on a pattern.

#### Example Usage
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, concat, current_date

# Initialize SparkSession
spark = SparkSession.builder.appName("example").getOrCreate()

# Sample DataFrame
data = [("Alice", 25), ("Bob", 30), ("Catherine", 35)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Add a constant column
df = df.withColumn("Country", lit("USA"))

# Add conditional logic
df = df.withColumn("AgeGroup", when(col("Age") > 30, "Above 30").otherwise("30 or Below"))

# Concatenate strings
df = df.withColumn("Description", concat(col("Name"), lit(" is "), col("Age"), lit(" years old")))

# Add current date
df = df.withColumn("Today", current_date())

df.show()

# Output:
# +---------+---+-------+-----------+---------------------+----------+
# |     Name|Age|Country|   AgeGroup|         Description|      Today|
# +---------+---+-------+-----------+---------------------+----------+
# |    Alice| 25|    USA|30 or Below|Alice is 25 years old|2025-01-04|
# |      Bob| 30|    USA|30 or Below|  Bob is 30 years old|2025-01-04|
# |Catherine| 35|    USA|   Above 30|Catherine is 35 years old|2025-01-04|
# +---------+---+-------+-----------+---------------------+----------+
```


### Optimization Techniques
1. **Caching and Persistence**:
   ```python
   rdd.cache()
   rdd.persist()
   ```
2. **Partitioning**: Ensure data is evenly distributed.
3. **Broadcast Variables**: Share read-only data efficiently.
   ```python
   broadcast_var = sc.broadcast([1, 2, 3])
   print(broadcast_var.value)
   ```
4. **Accumulator Variables**: Aggregate values across nodes.
   ```python
   accumulator = sc.accumulator(0)
   sc.parallelize([1, 2, 3, 4]).foreach(lambda x: accumulator.add(x))
   print(accumulator.value)  ## Output: 10
   ```

---

### Troubleshooting and Debugging
- **View Web UI**: Access Spark’s Web UI at `http://localhost:4040` for job details.
- **Logs**: Check Spark logs for errors.
- **Partitioning Issues**: Use `.glom()` to inspect partitions.

---

### Resources
- [Official Documentation](https://spark.apache.org/docs/latest/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- [Apache Spark GitHub Repository](https://github.com/apache/spark)

---

### Summary
PySpark is a powerful tool for big data processing. Its APIs for RDDs, DataFrames, SQL, MLlib, and Streaming provide flexibility and scalability. With proper optimization techniques, you can efficiently process vast datasets in a distributed environment.


