from pyspark.sql import SparkSession

from utils.postgres_utils import upsert_spark_df_to_postgres

spark = SparkSession.builder.getOrCreate()

# Create mock data based on user table
data = [
    # (1, "John Doe", "johndoe@example.com"),
    (1, "Vincent Chee", "vchee@example.com"),
    (2, "Jane Doe", "janedoe@example.com"),
    (3, "Alice", "alice@example.com"),
    (4, "Bob", "bob@example.com"),
    (5, "Charlie", "charlie@example.com"),
]
columns = ["id", "username", "email"]

# Create a dataframe
df = spark.createDataFrame(data, columns)

# Write the dataframe to the postgres table
upsert_spark_df_to_postgres(
    df,
    "users",
    ["id"],
    {
        "host": "postgres",
        "database": "postgres",
        "user": "admin",
        "password": "password",
        "port": 5432
    }
)

df = (
    spark.read.format("jdbc")
    .option(
        "url", "jdbc:postgresql://postgres:5432/postgres"
    )
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", "users")
    .option("user", "admin")
    .option("password", "password")
    .load()
)

df.show()
