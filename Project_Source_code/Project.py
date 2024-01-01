# Databricks notebook source
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, explode, when
spark = SparkSession.builder.appName("YelpDatasetPipeline").getOrCreate()

# COMMAND ----------

# Define aliases for DataFrames
business_alias = "business"
user_alias = "user"
review_alias = "review"
tip_alias = "tip"
checkin_alias = "checkin"

tip_df_aliased = tip_df.alias(tip_alias)
business_df_aliased = business_df.alias(business_alias)

# COMMAND ----------

def read_and_flatten_json(file_path: str, nested_columns: list = None) -> DataFrame:
    """
    Reads a JSON file into a DataFrame and flattens nested columns if they are provided.
    Parameters:
    - file_path (str): Path to the JSON file to be read.
    - nested_columns (list): List of strings representing the column names with nested structures to be flattened.
    Returns:
    - DataFrame: A Spark DataFrame with nested structures flattened.
    Raises:
    - Exception: If reading the JSON or flattening the nested columns fails.
    """
    try:
        df = spark.read.json(file_path)

        if nested_columns:
            for column_name in nested_columns:
                # Assuming nested column is of type Struct
                fields = df.schema[column_name].dataType.fields
                for field in fields:
                    df = df.withColumn(f"{column_name}_{field.name}", col(f"{column_name}.{field.name}"))
                df = df.drop(column_name)
        return df
    except Exception as e:
        print(f"Error in read_and_flatten_json for file {file_path}: {e}")
        raise

# COMMAND ----------

def clean_data(df: DataFrame, drop_cols: list = None, fill_defaults: dict = None, ensure_positive: list = None) -> DataFrame:
    """
    Cleans the provided DataFrame by dropping unnecessary columns, filling in default values for nulls,
    and ensuring certain columns have only positive values.
    Parameters:
    - df (DataFrame): The DataFrame to clean.
    - drop_cols (list): List of column names that should be dropped from the DataFrame.
    - fill_defaults (dict): Dictionary with column names as keys and default values as values.
    - ensure_positive (list): List of column names that should only have positive values.
    Returns:
    - DataFrame: A cleaned DataFrame with specified transformations applied.
    Raises:
    - Exception: If cleaning operations fail.
    """
    try:
        if drop_cols:
            df = df.drop(*drop_cols)
        if fill_defaults:
            for col_name, default_value in fill_defaults.items():
                df = df.fillna(default_value, col_name)
        if ensure_positive:
            for col_name in ensure_positive:
                df = df.withColumn(col_name, when(col(col_name) < 0, 0).otherwise(col(col_name)))
        return df
    except Exception as e:
        print(f"Error cleaning data: {e}")
        raise

# COMMAND ----------

def drop_rows_with_nulls(df: DataFrame, columns_to_check: list) -> DataFrame:
    """
    Drops rows that contain null values in specified columns.
    Parameters:
    - df (DataFrame): The DataFrame to process.
    - columns_to_check (list): List of column names to check for null values.
    Returns:
    - DataFrame: The DataFrame with rows containing nulls in specified columns dropped.
    """
    try:
        for column in columns_to_check:
            df = df.filter(col(column).isNotNull())
        return df
    except Exception as e:
        print(f"Error in dropping nulls: {e}")
        raise

# COMMAND ----------

def drop_duplicates(df: DataFrame, subset: list) -> DataFrame:
    """
    Drops duplicate rows from the DataFrame based on a subset of columns.
    Parameters:
    - df (DataFrame): The DataFrame from which duplicates need to be removed.
    - subset (list): List of column names to consider for identifying duplicates.
    Returns:
    - DataFrame: A DataFrame with duplicates removed.
    Raises:
    - Exception: If the deduplication process fails.
    """
    try:
        return df.dropDuplicates(subset)
    except Exception as e:
        print(f"Error in dropping duplicates: {e}")
        raise


# COMMAND ----------

def rename_duplicate_columns(df: DataFrame, suffixes=('_left', '_right')) -> DataFrame:
    """
    Renames duplicate columns in the DataFrame by appending a suffix to ensure column name uniqueness.
    Assumes the DataFrame has already been joined.
    Parameters:
    df (DataFrame): The DataFrame with potential duplicate column names.
    suffixes (tuple): A tuple containing the suffixes to append to duplicate column names.
    Returns:
    DataFrame: A DataFrame with renamed duplicate columns.
    Raises:
    - Exception: If renaming duplicate columns fails.
    """
    try:
        columns = df.columns
        duplicate_columns = [col for col in set(columns) if columns.count(col) > 1]
        new_columns = []

        for column in columns:
            if columns.count(column) > 1:  # If the column is duplicated
                new_column = f"{column}{suffixes[duplicate_columns.index(column) % 2]}"
                df = df.withColumnRenamed(column, new_column)
                new_columns.append(new_column)
            else:
                new_columns.append(column)

        return df
    except Exception as e:
        print(f"Error renaming duplicate columns: {e}")
        raise

# COMMAND ----------

def write_delta(df: DataFrame, path: str):
    """
    Writes a DataFrame to a specified path in Delta Lake format.
    Parameters:
    - df (DataFrame): The DataFrame to be written to Delta Lake.
    - path (str): The path in Delta Lake where the DataFrame will be written.
    Raises:
    - Exception: If writing to Delta Lake fails.
    """
    try:
        df.write.format('delta').mode('overwrite').save(path)
    except Exception as e:
        print(f"Error writing to Delta Lake at {path}: {e}")
        raise

# COMMAND ----------

try:
    # define file path
    business_json_path = "/FileStore/tables/yelp_data/yelp_data/yelp_academic_dataset_business.json"
    checkin_json_path = "/FileStore/tables/yelp_data/yelp_data/yelp_academic_dataset_checkin.json"
    review_json_path = [
        "/FileStore/tables/yelp_data/yelp_data/yelp_academic_dataset_review_split_1.json",
        "/FileStore/tables/yelp_data/yelp_data/yelp_academic_dataset_review_split_2.json",
        "/FileStore/tables/yelp_data/yelp_data/yelp_academic_dataset_review_split_3.json"
    ]
    tip_json_path = "/FileStore/tables/yelp_data/yelp_data/yelp_academic_dataset_tip.json"
    user_json_path = [
        "/FileStore/tables/yelp_data/yelp_data/yelp_academic_dataset_user_split_1.json",
        "/FileStore/tables/yelp_data/yelp_data/yelp_academic_dataset_user_split_2.json"
    ]
except Exception as e:
    print(f"Error in the file path pipeline: {e}")
    raise
    

# COMMAND ----------

try:
    # Load and process datasets
    # Read and clean data with alias
    business_df = read_and_flatten_json(business_json_path, nested_columns=["attributes"]).alias(business_alias)
    business_df = clean_data(business_df, drop_cols=["some_non_essential_column"])
    business_df = drop_rows_with_nulls(business_df, ["business_id"])

    user_df = spark.read.format('json').load(user_json_path).alias(user_alias)
    user_df = clean_data(user_df, fill_defaults={"name": "unknown"})
    user_df = drop_rows_with_nulls(user_df, ["user_id"])

    review_df = spark.read.format('json').load(review_json_path).alias(review_alias)
    review_df = clean_data(review_df, ensure_positive=["stars"])
    review_df = drop_rows_with_nulls(review_df, ["user_id", "business_id"])
    
    checkin_df = read_and_flatten_json(checkin_json_path)

    tip_df = read_and_flatten_json(tip_json_path)
    tip_df = drop_rows_with_nulls(tip_df, ["user_id", "business_id"])

except Exception as e:
    print(f"Error in the data processing pipeline: {e}")
    raise

# COMMAND ----------

try:
    # Load and process business data
    business_df = read_and_flatten_json(business_json_path, nested_columns=["attributes"]).alias(business_alias)
    business_df = clean_data(business_df, drop_cols=["some_non_essential_column"])
    business_df = drop_rows_with_nulls(business_df, ["business_id"])
    business_df = drop_duplicates(business_df, subset=["business_id"])
    
    # Load and process user data
    user_df = spark.read.format('json').load(user_json_path).alias(user_alias)
    user_df = clean_data(user_df, fill_defaults={"name": "unknown"})
    user_df = drop_rows_with_nulls(user_df, ["user_id"])
    user_df = drop_duplicates(user_df, subset=["user_id"])
    
    # Load and process review data
    review_df = spark.read.format('json').load(review_json_path).alias(review_alias)
    review_df = clean_data(review_df, ensure_positive=["stars"])
    review_df = drop_rows_with_nulls(review_df, ["user_id", "business_id"])
    review_df = drop_duplicates(review_df, subset=["review_id"])
    
    # Load and process checkin data
    checkin_df = read_and_flatten_json(checkin_json_path).alias(checkin_alias)
    checkin_df = drop_rows_with_nulls(checkin_df, ["business_id"])
    checkin_df = drop_duplicates(checkin_df, subset=["business_id"])
    
    # Load and process tip data
    tip_df = read_and_flatten_json(tip_json_path).alias(tip_alias)
    tip_df = drop_rows_with_nulls(tip_df, ["user_id", "business_id"])
except Exception as e:
    print(f"Error in the data processing pipeline: {e}")
    raise

# COMMAND ----------

try:
    business_df.write.format('delta').mode('overwrite').option("mergeSchema", "true").save("/FileStore/tables/Delta/business_delta")
    user_df.write.format('delta').mode('overwrite').option("mergeSchema", "true").save("/FileStore/tables/Delta/user_delta")
    review_df.write.format('delta').mode('overwrite').option("mergeSchema", "true").save("/FileStore/tables/Delta/review_delta")
    checkin_df.write.format('delta').mode('overwrite').option("mergeSchema", "true").save("/FileStore/tables/Delta/checkin_delta")
    tip_df.write.format('delta').mode('overwrite').option("mergeSchema", "true").save("/FileStore/tables/Delta/tip_delta")
except Exception as e:
    print(f"Error in delta writing pipeline: {e}")
    raise


# COMMAND ----------

# Alias columns to resolve ambiguities
review_df = review_df.withColumnRenamed('business_id', 'review_business_id') \
                     .withColumnRenamed('user_id', 'review_user_id')
                     
# Alias any potentially conflicting columns in business_df and user_df
business_df = business_df.withColumnRenamed('_corrupt_record', 'business_corrupt_record')
user_df = user_df.withColumnRenamed('_corrupt_record', 'user_corrupt_record')

# Perform the join
joined_df = review_df.join(business_df, review_df['review_business_id'] == business_df['business_id']) \
                     .join(user_df, review_df['review_user_id'] == user_df['user_id'])
                     
# Select all columns from the joined DataFrame
silver_df = joined_df.select(*joined_df.columns)

# Write the silver table
silver_table_path = "/FileStore/tables/Silver/silver_table_delta"
silver_df.write.format('delta').mode('overwrite').save(silver_table_path)

