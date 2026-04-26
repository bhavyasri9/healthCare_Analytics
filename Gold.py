# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.healthcareproject12.dfs.core.windows.net",
  "ym06SBsQIBn/EAc8VPR5ko550FeO0veax+l9s1V25piCu3AiFCVC+3VDEOSdHqhD0CRzQkypuN5B+AStV/tkHQ=="
)

# COMMAND ----------

from pyspark.sql.functions import *

appointments_df = spark.read.format("delta").load(
    "abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/silver/appointments"
)

patients_df = spark.read.format("delta").load(
    "abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/silver/patients"
)

doctors_df = spark.read.format("delta").load(
    "abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/silver/doctors"
)

departments_df = spark.read.format("delta").load(
    "abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/silver/departments"
)

billing_df = spark.read.format("delta").load(
    "abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/silver/billing"
)

diagnostics_df = spark.read.format("delta").load(
    "abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/silver/diagnostics"
)

feedback_df = spark.read.format("delta").load(
    "abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/silver/feedback"
)

# COMMAND ----------

gold_no_show = appointments_df.groupBy("department_id") \
    .agg(
        count("*").alias("total_appointments"),
        sum("no_show").alias("no_show_count")
    ) \
    .withColumn("no_show_rate", col("no_show_count") / col("total_appointments"))

# COMMAND ----------

gold_no_show.write.format("delta").mode("overwrite") \
    .save("abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/gold/gold_no_show_rate")

# COMMAND ----------

gold_no_show.display()

# COMMAND ----------

gold_doctor_util = appointments_df.groupBy("doctor_id") \
    .agg(count("*").alias("total_appointments"))

# COMMAND ----------

gold_doctor_util.write.format("delta").mode("overwrite") \
    .save("abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/gold/gold_doctor_utilization")

# COMMAND ----------

gold_doctor_util.display()

# COMMAND ----------

gold_dept_revenue = appointments_df.join(billing_df, "appointment_id") \
    .groupBy("department_id") \
    .agg(sum("bill_amount").alias("total_revenue"))

# COMMAND ----------

gold_dept_revenue.write.format("delta").mode("overwrite") \
    .save("abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/gold/gold_department_revenue")

# COMMAND ----------

gold_dept_revenue.display()

# COMMAND ----------

gold_wait_time = appointments_df \
    .withColumn("visit_month", date_format("appointment_day", "yyyy-MM")) \
    .groupBy("visit_month") \
    .agg(avg("waiting_days").alias("avg_waiting_days"))

# COMMAND ----------

gold_wait_time.write.format("delta").mode("overwrite") \
    .save("abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/gold/gold_wait_time_trends")

# COMMAND ----------

gold_wait_time.display()

# COMMAND ----------

patient_visit_counts = appointments_df.groupBy("patient_id") \
    .agg(count("*").alias("visit_count"))

gold_revisit = patient_visit_counts.agg(
    (sum(when(col("visit_count") > 1, 1).otherwise(0)) / count("*"))
    .alias("revisit_rate")
)

# COMMAND ----------

gold_revisit.write.format("delta").mode("overwrite") \
    .save("abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/gold/gold_patient_revisit_rate")

# COMMAND ----------

gold_revisit.display()

# COMMAND ----------

gold_diag_volume = diagnostics_df.groupBy("test_name") \
    .agg(count("*").alias("test_count"))

# COMMAND ----------

gold_diag_volume.write.format("delta").mode("overwrite") \
    .save("abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/gold/gold_diagnostics_volume")

# COMMAND ----------

gold_diag_volume.display()

# COMMAND ----------

gold_feedback = feedback_df.groupBy() \
    .agg(
        avg("rating").alias("avg_rating"),
        count("*").alias("total_feedbacks")
    )

# COMMAND ----------

gold_feedback.write.format("delta").mode("overwrite") \
    .save("abfss://healthcareproject@healthcareproject12.dfs.core.windows.net/gold/gold_feedback_summary")

# COMMAND ----------

gold_feedback.display()

# COMMAND ----------

peak_hours = appointments_df \
    .withColumn("hour", hour("scheduled_day")) \
    .groupBy("hour") \
    .agg(count("*").alias("appointments"))

# COMMAND ----------

gold_dept_revenue.orderBy(col("total_revenue").desc()).display()

# COMMAND ----------

