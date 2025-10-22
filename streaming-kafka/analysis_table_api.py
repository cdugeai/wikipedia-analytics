from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col
from pyflink.table import DataTypes
from pyflink.table.schema import Schema
from pyflink.table.expressions import *
from pyflink.table.window import Tumble, Slide, Session, Over
from pyflink.table.udf import udtf
import os

# Setup environment
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
table_env = TableEnvironment.create(env_settings)
jar_path = os.path.abspath("./jars/flink-sql-connector-kafka-4.0.1-2.0.jar")
table_env.get_config().get_configuration().set_string(
    "pipeline.jars", f"file:///{jar_path}"
)

table_env.execute_sql("""
    CREATE TABLE kafka_source_table (
        id STRING,
        title STRING,
        server_name STRING,
        bot BOOLEAN
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'wiki_data',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'praveen_group_1',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json'
    )
""")

# Display streaming data
streaming_data = table_env.sql_query(
    "SELECT id, title, server_name, bot FROM kafka_source_table"
)
# Count articles by country
count_countries = table_env.sql_query("""
    WITH src as (
        SELECT SPLIT_INDEX(server_name, '.', 0) as country FROM kafka_source_table
    )
    SELECT country, count(country) as n FROM src 
    WHERE country not like 'commons'
    group by country
""")

# Execute query
streaming_data.execute().print()
