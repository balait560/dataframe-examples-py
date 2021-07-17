from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, BooleanType,DoubleType
import os.path
import yaml

if __name__ == '__main__':
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read Files") \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4') \
        .getOrCreate()
        # .master('local[*]') \
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    print(current_dir)
    app_config_path = os.path.abspath(current_dir + "/../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    print("\nCreating dataframe ingestion CSV file using 'SparkSession.read.format()'")





    print("Creating dataframe ingestion CSV file using 'SparkSession.read.csv()',")

    finance_df = spark.read \
        .option("mode", "DROPMALFORMED") \
        .option("header", "false") \
        .option("delimiter", ",") \
        .option("inferSchema", "true") \
        .csv("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/cust_name.csv") \



    finance_df.printSchema()
    finance_df.show()

    finance_df \
         .write \
         .format("hive") \
         .mode("overwrite") \
         .insertInto("cust.employee")
    spark.stop()

# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" practice/hive_write.py
