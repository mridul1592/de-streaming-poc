import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster


def create_keyspace(session):
    # keyspace = like a database in cassandra, everything else lives inside it
    # replication_factor 1 is fine for local/dev, would bump this up in a real cluster
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)
    logging.info("Keyspace created successfully!")


def create_table(session):
    # table has to exist before spark tries to stream into it
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            dob TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT
        )
    """)
    logging.info("Table created successfully!")


def insert_data(session, **kwargs):
    # not actually used by the streaming job below (spark writes straight to cassandra
    # via the connector) but keeping this around for manual/one-off inserts + testing
    logging.info("Inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    post_code = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address,
                post_code, email, username, dob, registered_date, phone, picture)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address, post_code, email, username,
              dob, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")
    except Exception as e:
        logging.error(f"Could not insert data due to exception: {e}")


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


def create_cassandra_connection():
    try:
        # no auth provider needed for the default local cassandra image, add one here
        # (PlainTextAuthProvider) if the cluster ends up needing a username/password
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to exception: {e}")
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka, then shape the raw stream into a dataframe matching our table
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
            
            insert_data(session)

            # this is the actual streaming write - keeps running until you kill it.
            # checkpointLocation is required by spark so it can track what's already
            # been written and pick back up correctly if the job restarts
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                                .option('checkpointLocation', '/tmp/checkpoint')
                                .option('keyspace', 'spark_streams')
                                .option('table', 'created_users')
                                .start())

            streaming_query.awaitTermination()
