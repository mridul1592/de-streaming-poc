import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col

# runs inside the spark-master container (spark 3.5 / scala 2.12), so hosts are docker
# service names. the keyspace + table are created beforehand with cqlsh, because the
# spark image has no cassandra-driver.


def create_spark_connection():
    # the jars.packages config pulls in the cassandra + kafka connectors at runtime
    # (spark will download these from maven on first run, so first launch is slow)
    s_con = None
    try:
        s_con = SparkSession.builder \
                .appName('UserDataStreaming') \
                .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,"
                                                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
                .config('spark.cassandra.connection.host', "cassandra") \
                .getOrCreate()
        s_con.sparkContext.setLogLevel("ERROR")  # spark logs are way too noisy on INFO
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create spark connection due to exception: {e}")

    return s_con


def connect_to_kafka(spark_conn):
    # readStream + startingOffsets earliest = read everything from the beginning of
    # the topic, not just new messages. switch to 'latest' if that's not what you want
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka dataframe created successfully!")
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because: {e}")

    return spark_df


def create_selection_df_from_kafka(spark_df):
    # kafka gives us raw bytes in the 'value' column, so we cast to string then
    # parse the json against this schema to get actual columns out of it
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')) \
        .select("data.*")

    return sel


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka, then shape the raw stream into a dataframe matching our table
        spark_df = connect_to_kafka(spark_conn)

        if spark_df is not None:
            selection_df = create_selection_df_from_kafka(spark_df)

            # this is the actual streaming write - keeps running until you kill it.
            # checkpointLocation is required by spark so it can track what's already
            # been written and pick back up correctly if the job restarts
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                                .option('checkpointLocation', '/tmp/checkpoint')
                                .option('keyspace', 'spark_streams')
                                .option('table', 'created_users')
                                .start())

            streaming_query.awaitTermination()
