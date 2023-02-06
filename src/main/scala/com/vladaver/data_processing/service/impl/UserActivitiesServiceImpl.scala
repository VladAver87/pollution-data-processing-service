package com.vladaver.data_processing.service.impl

import com.vladaver.data_processing.schemas.Schemas.activitiesSchema
import com.vladaver.data_processing.service.UserActivitiesService
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}

class UserActivitiesServiceImpl(implicit sc: SQLContext) extends UserActivitiesService {

  override def calculateActivitiesStats(pathToDataset: String): DataFrame = {

    val activitiesData = readDataset(pathToDataset = pathToDataset)
    val totalActivitiesSplitedByTime = getTotalActivitiesSplitedByTime(data = activitiesData)
    val splitedActivities = splitActivitiesByWorkAndNotWork(data = totalActivitiesSplitedByTime)
    val aggregatedActivities = aggregateActiviesByWorkAndTotal(data = splitedActivities)
    aggregatedActivities
      .withColumn("max_activity", max(column("total_activity")).over())
      .withColumn("min_activity", min(column("total_activity")).over())
      .withColumn("avg_activity", avg(column("total_activity")).over())
      .withColumn("is_work_square", when(
        column("total_worktime_activity").between(
          column("avg_activity"), column("max_activity")), true
      ).otherwise(false))
      .drop("total_worktime_activity", "total_activity")
  }

  private def aggregateActiviesByWorkAndTotal(data: DataFrame): DataFrame = {
    data.withColumn("total_worktime_activity", when(
      column("is_working_time").equalTo(true), column("total_activity"))
      .otherwise(0)
    )
      .select(
        column("square_id"),
        column("total_worktime_activity"),
        column("total_activity")
      )
      .groupBy(column("square_id"))
      .agg(
        sum(column("total_worktime_activity")).as("total_worktime_activity"),
        sum(column("total_activity")).as("total_activity")
      )
  }

  private def splitActivitiesByWorkAndNotWork(data: DataFrame): DataFrame = {
    data.select(
      column("square_id"),
      column("total_activity"),
      column("is_working_time")
    )
      .groupBy(column("square_id"), column("is_working_time"))
      .agg(
        sum(column("total_activity")).as("total_activity")
      )
  }

  private def getTotalActivitiesSplitedByTime(data: DataFrame): DataFrame = {
    data.select(
        column("square_id"),
        from_unixtime(column("time_interval").divide(1000)).as("time_interval"),
        column("sms_in_activity"),
        column("sms_out_activity"),
        column("call_in_activity"),
        column("call_out_activity"),
        column("inet_traffic_activity"))
      .groupBy(column("square_id"),
        window(column("time_interval"), "8 hours", "8 hours", "5 hours")
          .as("time_interval"))
      .agg(
        sum("sms_in_activity").as("sum_sms_in_activity"),
        sum("sms_out_activity").as("sum_sms_out_activity"),
        sum("call_in_activity").as("sum_call_in_activity"),
        sum("call_out_activity").as("sum_call_out_activity"),
        sum("inet_traffic_activity").as("sum_inet_traffic_activity")
      )
      .withColumn("total_activity",
        column("sum_sms_in_activity") +
          column("sum_sms_out_activity") +
          column("sum_call_in_activity") +
          column("sum_call_out_activity") +
          column("sum_inet_traffic_activity"))
      .withColumn("is_working_time", when(
        hour(col("time_interval.start")).equalTo(9)
          .and(hour(col("time_interval.end")).equalTo(17)), true)
        .otherwise(false))
      .drop("sms_in_activity", "sms_out_activity", "call_in_activity", "call_out_activity",
        "inet_traffic_activity", "sum_sms_in_activity", "sum_sms_out_activity", "sum_call_in_activity",
        "sum_call_out_activity", "sum_inet_traffic_activity", "time_interval")
      .orderBy("square_id")
  }

  private def readDataset(pathToDataset: String): DataFrame = {
    sc.read
      .options(Map("sep" -> "\t", "header" -> "false"))
      .schema(activitiesSchema)
      .csv(path = pathToDataset)
  }
}

