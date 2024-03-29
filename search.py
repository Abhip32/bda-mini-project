from pyspark import SparkContext, SparkConf
# Create a Spark context
conf = SparkConf().setAppName("SearchElement").setMaster("local")
sc = SparkContext(conf=conf)
# Define the data to be searched
data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
# Parallelize the data into RDD (Resilient Distributed Dataset)
rdd = sc.parallelize(data)
# Define the search function
def search_element(element):
  return element == 11 # Change the search element as needed
# Map function to search for the element in the dataset
result = rdd.map(search_element)
# Collect the results
search_result = result.collect()
# Print the search result
print("Abhishek Patil/BDA/CA-2")
if True in search_result:
  print("Element found in the dataset")
else:
  print("Element not found in the dataset")
# Stop the Spark context
sc.stop()