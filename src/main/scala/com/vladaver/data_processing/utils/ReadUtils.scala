package com.vladaver.data_processing.utils

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.StructType

object ReadUtils {

  def readDataset(path: String, schema: StructType, sep: String = "\t")(implicit sc: SQLContext): DataFrame = {
    sc.read
      .options(Map("sep" -> sep, "header" -> "false"))
      .schema(schema)
      .csv(path = path)
  }
}
