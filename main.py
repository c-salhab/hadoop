from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, avg
# create db if it doesn't exist
spark = SparkSession.builder.appName("AnalyseCardio").getOrCreate()

# read dataset from hdfs
# inferSchema : essayer de determiner automatiquement le type de donnees de chaque colonne en analysant les donnees
dataset_file = (spark.read.option("sep", ";").option("header", "true").option("inferSchema", "true")
                .csv("hdfs://localhost:9000/user/annan/cardio_train.csv"))

# convert age to int and create age categories
dataset_file = dataset_file.withColumn("age_years", col("age").cast("int") / 365.25)
dataset_file = dataset_file.withColumn("age_category",
                                       when(col("age_years") < 30, "20-29")
                                       .when((col("age_years") >= 30) & (col("age_years") < 40), "30-39")
                                       .when((col("age_years") >= 40) & (col("age_years") < 50), "40-49")
                                       .when((col("age_years") >= 50) & (col("age_years") < 60), "50-59")
                                       .otherwise("60+"))

# convert the gender from boolean to str
dataset_file = dataset_file.withColumn("gender", when(col("gender") == "2", "Homme").otherwise("Femme"))

# convert cholesterol to categories
dataset_file = dataset_file.withColumn("cholesterol_category",
                                       when(col("cholesterol") == 1, "normal")
                                       .when(col("cholesterol") == 2, "above normal")
                                       .when(col("cholesterol") == 3, "well above normal")
                                       .otherwise("unknown"))

# convert glucose to categories
dataset_file = dataset_file.withColumn("glucose_category",
                                       when(col("gluc") == 1, "normal")
                                       .when(col("gluc") == 2, "above normal")
                                       .when(col("gluc") == 3, "well above normal")
                                       .otherwise("unknown"))

# perform the analysis for cholesterol
cholesterol_result = dataset_file.groupBy("age_category", "gender", "cholesterol_category") \
    .agg(count("*").alias("count")) \
    .orderBy("age_category", "gender", "cholesterol_category")

# perform the analysis for glucose
glucose_result = dataset_file.groupBy("age_category", "gender", "glucose_category") \
    .agg(count("*").alias("count")) \
    .orderBy("age_category", "gender", "glucose_category")

# Analysis of the impact of lifestyle habits on blood pressure
lifestyle_impact = dataset_file.groupBy("age_category", "gender", "smoke", "alco", "active") \
    .agg(
        avg("ap_hi").alias("avg_systolic_bp"),
        avg("ap_lo").alias("avg_diastolic_bp") auq
    ) \
    .orderBy("age_category", "gender", "smoke", "alco", "active")

# Store results in CSV file
lifestyle_impact_df = lifestyle_impact.toPandas()
lifestyle_impact_df.to_csv("lifestyle_impact_on_bp.csv", index=False)


# store results in csv files
pandas_df1 = cholesterol_result.toPandas()
pandas_df1.to_csv("cholesterol_result.csv", index=False)

pandas_df1 = glucose_result.toPandas()
pandas_df1.to_csv("glucose_result.csv", index=False)

spark.stop()