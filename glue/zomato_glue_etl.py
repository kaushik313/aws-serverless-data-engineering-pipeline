import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, trim, when, avg, count, desc

# Read job parameters
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "INPUT_PATH", "OUTPUT_PATH"]
)

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Remove extra spaces and quotes from paths
input_path = args["INPUT_PATH"].strip().strip('"').strip("'")
output_path = args["OUTPUT_PATH"].strip().strip('"').strip("'")

if not output_path.endswith("/"):
    output_path = output_path + "/"

# Read CSV file from S3
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(input_path)
)

# Clean and transform data
df_clean = (
    df
    .withColumnRenamed("name_of_restaurant", "restaurant_name")
    .withColumnRenamed("features/category", "category")
    .withColumnRenamed("area/location", "area_location")
    .withColumn("restaurant_name", trim(col("restaurant_name")))
    .withColumn("category", trim(col("category")))
    .withColumn("market_segment", trim(col("market_segment")))
    .withColumn("cuisine", trim(col("cuisine")))
    .withColumn("top_dishes", trim(col("top_dishes")))
    .withColumn("address", trim(col("address")))
    .withColumn("area_location", trim(col("area_location")))
    .withColumn("dining_rating", col("dining_rating").cast("double"))
    .withColumn("latitude", col("latitude").cast("double"))
    .withColumn("longitude", col("longitude").cast("double"))
    .filter(col("restaurant_name").isNotNull())
    .filter(col("dining_rating").isNotNull())
)

# Create rating category
df_clean = (
    df_clean
    .withColumn(
        "rating_category",
        when(col("dining_rating") >= 4.5, "Excellent")
        .when(col("dining_rating") >= 4.0, "Very Good")
        .when(col("dining_rating") >= 3.5, "Good")
        .otherwise("Average")
    )
)

# Save cleaned data as Parquet
df_clean.write.mode("overwrite").parquet(
    output_path + "cleaned_zomato_data/"
)

# Area summary
area_summary = (
    df_clean
    .groupBy("area_location")
    .agg(
        count("*").alias("restaurant_count"),
        avg("dining_rating").alias("average_rating")
    )
    .orderBy(desc("average_rating"))
)

area_summary.write.mode("overwrite") \
    .option("header", "true") \
    .parquet(output_path + "area_summary/")

# Cuisine summary
cuisine_summary = (
    df_clean
    .groupBy("cuisine")
    .agg(
        count("*").alias("restaurant_count"),
        avg("dining_rating").alias("average_rating")
    )
    .orderBy(desc("restaurant_count"))
)

cuisine_summary.write.mode("overwrite") \
    .option("header", "true") \
    .parquet(output_path + "cuisine_summary/")

# Top restaurants
top_restaurants = (
    df_clean
    .select(
        "restaurant_name",
        "category",
        "market_segment",
        "cuisine",
        "area_location",
        "dining_rating",
        "rating_category",
        "zomato_url"
    )
    .orderBy(desc("dining_rating"))
)

top_restaurants.write.mode("overwrite") \
    .option("header", "true") \
    .parquet(output_path + "top_restaurants/")

job.commit()
