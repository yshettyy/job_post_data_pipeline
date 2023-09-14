from os.path import abspath
from pyspark.sql import SparkSession

warehouse_location = abspath("spark-warehouse")

spark = (
    SparkSession.builder.appName("Job processing")
    .config("spark.sql.warehouse.dir", warehouse_location)
    .enableHiveSupport()
    .getOrCreate()
)

df = spark.read.option("header", "true").csv("hdfs://namenode:9000/job/job.csv")
job_posting = df.select(
    "linkedin_job_url_cleaned",
    "company_name",
    "company_url",
    "job_title",
    "job_location",
    "posted_date",
)
job_posting.write.mode("append").insertInto("job_posting")
