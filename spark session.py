#import spark session
from pyspark.sql import SparkSession



#Inicialize a sparksession
spark = SparkSession.builder.appName("MySparkApp").getOrCreate()

#.builder() sets up a session
#get0rCreate() creates or retrieves a session
#.appname() helps manage multiple sessions

#Create a DataFrame
census_df = spark.read.csv("census.csv",["gender","age","zipcode","salary_range_usd"
"mariage_status"])

#Show DataFrame

census_df.show()

#Create a a DataFrame from CSV
census_df2 = spark.read.csv('path/to/census.csv'), header = True, inferSchema = True
#read.csv much faster than 'createDataFrame()'

#show the schema
census_df2.printSchema()


# .count() will return the total row numbers in the DataFrame
row_count = census_df2.count()
print(f'Number of rows: {row_count}')

#.groupby() allows the use of sql-like aggregations
census_df2.groupBy('gender').agg({'salary_usd': 'avg'}).show()


#KEY FUNCTIONS for PySpark analitcs
#.select(): Selects specific columns from DataFrame
#.filter(); Filters rows based on specific conditions
#.groupBy(): Groups rows based on one or more columns
#.agg(): Appliesa aggregate functions to grouped data   


#import the necessary types as classes
from pyspark.sql.types import (StructType, StructField, IntegerType, StringType, ArrayType)


#construct a schema
schema = StructType([
    StructField('id', IntegerType(),True),
    StructField('name', StringType(),True),
    StructField('scores',ArrayType(IntegerType()),True),
])

#set the schema
df = spark.createDataFrame(data, schema=schema)

#.sort() to order by a collection of columns
#where to filter match a specific value

df.select('name','age').show()

df.filter(df['age'] > 30).show( )

df.where(df['age'] == 30).show()






from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# Fill in the schema with the columns you need from the exercise instructions
schema = StructType([StructField("age",IntegerType()),
                     StructField("education_num",IntegerType()),
                     StructField("marital_status",StringType()),
                     StructField("occupation",StringType()),
                     StructField("income",StringType()),
                    ])

# Read in the CSV, using the schema you defined above
census_adult = spark.read.csv("adult_reduced_100.csv", sep=',', header=False, schema=schema)

# Print out the schema
census_adult.printSchema()