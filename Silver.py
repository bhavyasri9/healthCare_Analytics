# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.healthcareproject12.dfs.core.windows.net",
  "ym06SBsQIBn/EAc8VPR5ko550FeO0veax+l9s1V25piCu3AiFCVC+3VDEOSdHqhD0CRzQkypuN5B+AStV/tkHQ=="
)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

bronze_df = spark.read.format("delta").load(
    "abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/bronze/healthcare_bronze"
)

# COMMAND ----------

bronze_df.display()

# COMMAND ----------

from pyspark.sql.functions import *

clean_df = bronze_df.select(
    col("PatientId").alias("patient_id"),
    col("AppointmentID").alias("appointment_id"),
    col("Gender").alias("gender"),
    col("ScheduledDay").alias("scheduled_day"),
    col("AppointmentDay").alias("appointment_day"),
    col("Age").alias("age"),
    col("Neighbourhood").alias("neighbourhood"),
    col("Scholarship").alias("scholarship"),
    col("Hipertension").alias("hypertension"),
    col("Diabetes").alias("diabetes"),
    col("Alcoholism").alias("alcoholism"),
    col("Handcap").alias("handicap"),
    col("SMS_received").alias("sms_received"),
    col("No-show").alias("no_show")
)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

clean_df = clean_df \
    .withColumn("scheduled_day", to_timestamp("scheduled_day")) \
    .withColumn("appointment_day", to_timestamp("appointment_day"))

# COMMAND ----------

from pyspark.sql.functions import when

clean_df = clean_df.withColumn(
    "gender",
    when(col("gender") == "M", "Male")
    .when(col("gender") == "F", "Female")
    .otherwise("Unknown")
)

# COMMAND ----------

clean_df = clean_df.withColumn(
    "no_show",
    when(col("no_show") == "Yes", 1).otherwise(0)
)

# COMMAND ----------

clean_df = clean_df.fillna({
    "scholarship": 0,
    "hypertension": 0,
    "diabetes": 0,
    "alcoholism": 0,
    "handicap": 0,
    "sms_received": 0
})

# COMMAND ----------

clean_df = clean_df.dropDuplicates(["appointment_id"])

# COMMAND ----------

from pyspark.sql.functions import datediff

clean_df = clean_df.withColumn(
    "waiting_days",
    datediff(col("appointment_day"), col("scheduled_day"))
)

# COMMAND ----------

clean_df.printSchema()
clean_df.display()

# COMMAND ----------

clean_df.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/silver/clean_appointments")

# COMMAND ----------

silver_df = spark.read.format("delta").load(
    "abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/silver/clean_appointments"
)

# COMMAND ----------

patients_df = silver_df.select(
    "patient_id",
    "gender",
    "age",
    "neighbourhood",
    "scholarship",
    "hypertension",
    "diabetes",
    "alcoholism"
).dropDuplicates(["patient_id"])

# COMMAND ----------

patients_df.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/silver/patients")

# COMMAND ----------

patients_df.display()

# COMMAND ----------

from pyspark.sql.functions import rand, floor

appointments_df = silver_df.select(
    "appointment_id",
    "patient_id",
    "scheduled_day",
    "appointment_day",
    "sms_received",
    "no_show",
    "waiting_days"
)

# COMMAND ----------

appointments_df = appointments_df \
    .withColumn("doctor_id", (floor(rand()*5) + 101)) \
    .withColumn("department_id", (floor(rand()*5) + 1))

# COMMAND ----------

appointments_df.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/silver/appointments")

# COMMAND ----------

appointments_df.display()

# COMMAND ----------

departments_data = [
    (1, "Cardiology"),
    (2, "Orthopedics"),
    (3, "General Medicine"),
    (4, "Neurology"),
    (5, "Pediatrics")
]

departments_df = spark.createDataFrame(
    departments_data,
    ["department_id", "department_name"]
)

# COMMAND ----------

departments_df.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/silver/departments")

# COMMAND ----------

departments_df.display()

# COMMAND ----------

doctors_data = [
    (101, "Dr. Sharma", 1, "Cardiologist"),
    (102, "Dr. Reddy", 2, "Orthopedic"),
    (103, "Dr. Khan", 3, "Physician"),
    (104, "Dr. Patel", 4, "Neurologist"),
    (105, "Dr. Mehta", 5, "Pediatrician")
]

doctors_df = spark.createDataFrame(
    doctors_data,
    ["doctor_id", "doctor_name", "department_id", "specialization"]
)

# COMMAND ----------

doctors_df.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/silver/doctors")

# COMMAND ----------

doctors_df.display()

# COMMAND ----------

from pyspark.sql.functions import round

billing_df = appointments_df.select("appointment_id") \
    .withColumn("bill_id", col("appointment_id") + 1000) \
    .withColumn("bill_amount", round(rand()*5000 + 500, 2)) \
    .withColumn("payment_status", (rand() > 0.2).cast("string"))

# COMMAND ----------

billing_df.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/silver/billing")

# COMMAND ----------

billing_df.display()

# COMMAND ----------

diagnostics_df = appointments_df.select("appointment_id") \
    .withColumn("diagnostic_id", col("appointment_id") + 2000) \
    .withColumn("test_name", (rand() > 0.5).cast("string")) \
    .withColumn("test_result", (rand() > 0.5).cast("string"))

# COMMAND ----------

diagnostics_df.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/silver/diagnostics")

# COMMAND ----------

diagnostics_df.display()

# COMMAND ----------

feedback_df = appointments_df.select("appointment_id") \
    .withColumn("feedback_id", col("appointment_id") + 3000) \
    .withColumn("rating", (floor(rand()*5) + 1)) \
    .withColumn("comments", (rand() > 0.5).cast("string"))

# COMMAND ----------

feedback_df.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/silver/feedback")

# COMMAND ----------

feedback_df.display()

# COMMAND ----------

