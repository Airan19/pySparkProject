import psycopg2
import pyspark
from psycopg2 import Error
from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import DateType
# from pyspark.sql import Row

# try:
#     # Connect to an existing database
#     connection = psycopg2.connect(user="postgres",password="#it's4Postgres",host="127.0.0.1",port="5433",database="postgres")
#     # Create a cursor to perform database operations
#     cursor = connection.cursor()
#     # Executing a SQL query
#     cursor.execute("SELECT * FROM public.ratings LIMIT 10;")
#     record = cursor.fetchall()
#     print(record)
#
#
#
# except (Exception, Error) as error:
#     print("Error while connecting to PostgreSQL", error)
# finally:
#     if (connection):
#         cursor.close()
#         connection.close()
#         print("PostgreSQL connection is closed")

spark = SparkSession.builder.config("spark.jars", "/C:/Users/aryan/Downloads/postgresql-42.6.0.jar").master("local").appName("PySpark_Postgres_test").getOrCreate()
spark.conf.set("spark.sql.parquet.compression.codec", "gzip")

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5433/postgres") \
    .option("dbtable", "ratings") \
    .option("user", "postgres") \
    .option("password", "#it's4Postgres") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_csv = spark.read.csv('C:/Users/aryan/Desktop/CHROME_DOWNLOADS/movies.csv', header='true', inferSchema='true')
# df_csv.show()
# df.show()

merged_df = df.join(df_csv,["movieId"])
new_df = merged_df.withColumn('date', func.from_unixtime('timestamp').cast(DateType()))
new_df = new_df.withColumn('year', func.date_format('date', 'yyyy')).withColumn("month", func.date_format('date', 'MM'))
new_df = new_df.drop('timestamp')
# new_df.show()

# using PartitionBy with two columns creating partitions
# new_df.write.format('parquet').partitionBy('year', 'month').mode('overwrite').saveAsTable('movie_dataset')
# new_df.write.partitionBy('year').parquet('/movie_data_files_1')

# using BucketBy
new_df.write.format('parquet').bucketBy(7,'userId').mode('overwrite').saveAsTable('bucketedData')
# Read parquet file
# parDF=spark.read.parquet("C:/movie_data_files_1/year=1996/part-00000-9a8e902b-1318-4c2f-8590-49bb02a07da8.c000.gz.parquet")
# parDF.show()