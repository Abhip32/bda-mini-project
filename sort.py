from pyspark.sql import SparkSession
# Create a Spark session
spark = SparkSession.builder \
.appName("SortData") \
.getOrCreate()
# Define dummy input data
dummy_data = [
"3\tCow",
"1\tDog",
"2\tCat",
"4\tBuffalo"
]
# Create RDD from dummy data
data_rdd = spark.sparkContext.parallelize(dummy_data)
# Sort the data
sorted_data = data_rdd.sortBy(lambda x: x.split('\t')[0])
# Collect and print the sorted data
sorted_results = sorted_data.collect()
print("Abhishek Patil/BDA/CA-2")
for result in sorted_results:
  print(result)
# Stop the Spark session
spark.stop()