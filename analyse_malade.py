"""
Charbel Salhab
3iabd1 - ESGI
07/2024
Projet Hadoop
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum

# create db if it doesn't exist
spark = SparkSession.builder.appName("AnalyseCardio").getOrCreate()

# read dataset from hdfs
# inferSchema : essayer de determiner automatiquement le type de donnees de chaque colonne en analysant les donnees
dataset_file = (spark.read.option("sep", ";").option("header", "true").option("inferSchema", "true")
                .csv("hdfs://0.0.0.0:9000/user/hadoop/dataset/dataset.csv"))

# convert age to int and create age categories
dataset_file = dataset_file.withColumn("age_years", col("age").cast("int") / 365.25)
dataset_file = dataset_file.withColumn("age_category",
                                       when(col("age_years") < 30, "20-29")
                                       .when((col("age_years") >= 30) & (col("age_years") < 40), "30-39")
                                       .when((col("age_years") >= 40) & (col("age_years") < 50), "40-49")
                                       .when((col("age_years") >= 50) & (col("age_years") < 60), "50-59")
                                       .otherwise("60+"))

# convert the gender from boolean to str
dataset_file = dataset_file.withColumn("gender", when(col("gender") == 2, "Homme").otherwise("Femme"))

# convert cholesterol to categories
dataset_file = dataset_file.withColumn("cholesterol_category",
                                       when(col("cholesterol") == 1, "1 normal")
                                       .when(col("cholesterol") == 2, "2 above normal")
                                       .when(col("cholesterol") == 3, "3 well above normal")
                                       .otherwise("unknown"))

# convert glucose to categories
dataset_file = dataset_file.withColumn("glucose_category",
                                       when(col("gluc") == 1, "1 normal")
                                       .when(col("gluc") == 2, "2 above normal")
                                       .when(col("gluc") == 3, "3 well above normal")
                                       .otherwise("unknown"))

# perform the analysis for cholesterol
cholesterol_result = dataset_file.groupBy("age_category", "gender", "cholesterol_category") \
    .agg(count("*").alias("count"), sum(when(col("cardio") == 1, 1).otherwise(0)).alias("cardio_disease")) \
    .orderBy("age_category", "gender", "cholesterol_category")

# perform the analysis for glucose
glucose_result = dataset_file.groupBy("age_category", "gender", "glucose_category") \
    .agg(count("*").alias("count"), sum(when(col("cardio") == 1, 1).otherwise(0)).alias("cardio_disease")) \
    .orderBy("age_category", "gender", "glucose_category")

# store results in csv files
cholesterol_result.toPandas().to_csv("cholesterol_result_cardio.csv", index=False)
glucose_result.toPandas().to_csv("glucose_result_cardio.csv", index=False)

spark.stop()
