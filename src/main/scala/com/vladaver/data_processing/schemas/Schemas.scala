package com.vladaver.data_processing.schemas

import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, LongType, StringType, StructType}

object Schemas {
  val activitiesSchema: StructType = new StructType()
    .add("square_id", IntegerType)
    .add("time_interval", LongType)
    .add("country_code", IntegerType)
    .add("sms_in_activity", FloatType)
    .add("sms_out_activity", FloatType)
    .add("call_in_activity", FloatType)
    .add("call_out_activity", FloatType)
    .add("inet_traffic_activity", FloatType)

  val pollutionLegendSchema: StructType = new StructType()
    .add("sensor_id", LongType)
    .add("sensor_street_name", StringType)
    .add("sensor_lat", FloatType)
    .add("sensor_long", FloatType)
    .add("sensor_type", StringType)
    .add("uom", StringType)
    .add("time_instant_format", StringType)


  val pollutionMISchema: StructType = new StructType()
    .add("sensor_id", LongType)
    .add("time_instant", StringType)
    .add("measurement", LongType)

}
