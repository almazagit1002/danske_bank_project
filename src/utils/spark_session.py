import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession


load_dotenv()

def get_spark(app_name):
    spark = (
        SparkSession.builder
        .appName(app_name)
        #how much mempory can I use?
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        
        # 2. PARALLELISM: how many cores do i have?
        # This allows Spark to process more 'chunks' of the CSV at once
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        
  
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4," 
                                    "com.amazonaws:aws-java-sdk-bundle:1.12.262,") 
        .getOrCreate()
    )

    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()

    # PULL FROM ENVIRONMENT
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    if not access_key or not secret_key:
        raise ValueError("AWS Credentials not found in environment or .env file!")

    hadoop_conf.set("fs.s3a.access.key", access_key)
    hadoop_conf.set("fs.s3a.secret.key", secret_key)
    
    # keep your existing setInt and endpoint configs below ...
    hadoop_conf.set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
    hadoop_conf.setInt("fs.s3a.connection.timeout", 60000)
    
    return spark