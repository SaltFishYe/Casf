package com.saltfish.utils

import org.apache.spark.sql.SparkSession

object SparkSessionUtils {
  def createSparkSession(isLocal: Boolean, appName: String): SparkSession = {
    val sparkSession = SparkSession.builder()
      .appName(appName)
      .config("spark.sql.warehouse.dir", "hdfs://ns1/sparkSQL/warehouse")

    if (isLocal){
      sparkSession.master("local")
    }
    sparkSession.getOrCreate()
  }
}
