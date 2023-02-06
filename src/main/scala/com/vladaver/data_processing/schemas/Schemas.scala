package com.vladaver.data_processing.schemas

import org.apache.spark.sql.types.{ArrayType, FloatType, IntegerType, LongType, StructType}

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

}
