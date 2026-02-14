from pyspark.sql import SparkSession

# Create Spark session with BigQuery connector
spark = SparkSession.builder \
    .appName("GCS to BigQuery") \
    .getOrCreate()

# ---------------------------
# 1. Read file from GCS
# ---------------------------

account_input_path = "gs://rameshsamplebucket/Datafile_accounts.csv"
branch_input_path = "gs://rameshsamplebucket/Datafile_branches.csv"
customer_input_path = "gs://rameshsamplebucket/Datafile_customers.csv"
products_input_path = "gs://rameshsamplebucket/Datafile_products.csv"
transaction_input_path = "gs://rameshsamplebucket/Datafile_transactions.csv"

account_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(account_input_path)

branch_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(branch_input_path)

customer_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(customer_input_path)

products_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(products_input_path)

transaction_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(transaction_input_path)

account_df.show()
branch_df.show()
customer_df.show()
products_df.show()
transaction_df.show()

# ---------------------------
# 2. Write to BigQuery
# ---------------------------

account_df.write \
  .format("bigquery") \
  .option("table", "ranjanrishi-project.testdataset.account") \
  .option("temporaryGcsBucket", "ramtempbucket7") \
  .mode("overwrite") \
  .save()

branch_df.write \
  .format("bigquery") \
  .option("table", "ranjanrishi-project.testdataset.branch") \
  .option("temporaryGcsBucket", "ramtempbucket7") \
  .mode("overwrite") \
  .save()

customer_df.write \
  .format("bigquery") \
  .option("table", "ranjanrishi-project.testdataset.customer") \
  .option("temporaryGcsBucket", "ramtempbucket7") \
  .mode("overwrite") \
  .save()

products_df.write \
  .format("bigquery") \
  .option("table", "ranjanrishi-project.testdataset.products") \
  .option("temporaryGcsBucket", "ramtempbucket7") \
  .mode("overwrite") \
  .save()

transaction_df.write \
  .format("bigquery") \
  .option("table", "ranjanrishi-project.testdataset.transaction") \
  .option("temporaryGcsBucket", "ramtempbucket7") \
  .mode("overwrite") \
  .save()
print("Data successfully loaded to BigQuery")