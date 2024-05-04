from pyspark.sql import SparkSession
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import col, lit, explode, when, monotonically_increasing_id
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Movies Data Transformation") \
    .getOrCreate()

def load_data_to_bronze(file_paths):
    """
    Load data from movie files into a bronze Delta table.
    """
    df = spark.read.format("json").load(file_paths)
    df = df.withColumn("status", lit("new"))  # Add a new column 'status' with initial value 'new'
    df.write.format("delta").mode("append").save("/mnt/delta/bronze")


def quarantine_negative_runtime():
    """
    Identify records with negative runtime and mark them as quarantined.
    """
    df = spark.read.format("delta").load("/mnt/delta/bronze")
    quarantined = df.filter(col("RunTime") < 0)
    quarantined.write.format("delta").mode("overwrite").save("/mnt/delta/quarantine")

    # Update bronze table marking quarantined records
    clean_df = df.withColumn("quarantined", col("RunTime") < 0)
    clean_df.write.format("delta").mode("overwrite").save("/mnt/delta/bronze")

def deduplicate_bronze():
    """
    Deduplicate entries in the bronze table based on the 'Id' field.
    """
    df = spark.read.format("delta").load("/mnt/delta/bronze")
    dedup_df = df.dropDuplicates(["Id"])
    dedup_df.write.format("delta").mode("overwrite").save("/mnt/delta/bronze")
def create_silver_tables():
    """
    Generate silver tables from the bronze table.
    """
    df = spark.read.format("delta").load("/mnt/delta/bronze")
    df = df.filter((col("quarantined") == False) & (col("status") == "new"))

    # Fix negative runtime in quarantine
    quarantined_df = spark.read.format("delta").load("/mnt/delta/quarantine")
    fixed_runtime_df = quarantined_df.withColumn("RunTime", spark_abs(col("RunTime")))
    fixed_runtime_df.write.format("delta").mode("overwrite").save("/mnt/delta/quarantine_fixed")

    # Create Movie table and enforce minimum budget
    movies_df = df.select("Id", "Title", "Overview", "Tagline",
                          when(col("Budget") < 1000000, 1000000).otherwise(col("Budget")).alias("Budget"),
                          "Revenue", "ImdbUrl", "TmdbUrl",
                          "PosterUrl", "BackdropUrl", "ReleaseDate", "RunTime", "Price")
    movies_df.write.format("delta").mode("overwrite").save("/mnt/delta/silver/movies")

    # Update the status in the bronze table to 'loaded'
    updated_status = df.withColumn("status", lit("loaded"))
    updated_status.write.format("delta").mode("overwrite").save("/mnt/delta/bronze")

    # Create Genres table by exploding the nested genres array
    genres_df = df.withColumn("genre", explode("genres")).select(
        col("genre.id").alias("GenreId"),
        when(col("genre.name").isNull(), "Unknown").otherwise(col("genre.name")).alias("genreName")
    ).distinct()
    genres_df.write.format("delta").mode("overwrite").save("/mnt/delta/silver/genres")

    # Create OriginalLanguages table
    languages_df = df.select("LanguageName").distinct()
    languages_df.withColumn("LanguageId", monotonically_increasing_id())
    languages_df.write.format("delta").mode("overwrite").save("/mnt/delta/silver/languages")


def main():
    def main():
        file_paths = [
            file for file in os.listdir('./') if file.endswith('.json')
        ]
        load_data_to_bronze(file_paths)
        quarantine_negative_runtime()
        deduplicate_bronze()
        create_silver_tables()


if __name__ == "__main__":
    main()