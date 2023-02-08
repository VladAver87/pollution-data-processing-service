package com.vladaver.data_processing

import com.vladaver.data_processing.Pipeline.ExtendedDataFrame
import com.vladaver.data_processing.liquibase.SchemaMigration
import com.vladaver.data_processing.utils.Utils.loadConfigs
import org.apache.spark.sql.{SQLContext, SparkSession}

object PollutionDataProcessingApp {

  def main(args: Array[String]): Unit = {

    val config = loadConfigs()
    val sparkSession: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("PollutionDataProcessingApp")
      .getOrCreate()
    implicit val sc: SQLContext = sparkSession.sqlContext

    new SchemaMigration(config).run()

    val pipeline = new Pipeline(config)
    try {
      pipeline.calculateAggregatedData()
        .generateResultReports(config)
    } finally {
      sc.sparkContext.stop()
    }
  }

}
