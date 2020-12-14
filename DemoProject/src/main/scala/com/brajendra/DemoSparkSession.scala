package com.brajendra

import org.apache.spark.sql.SparkSession


object DemoSparkSession {
  
  def _sparkSession():SparkSession = {
    
    SparkSession.builder().appName("DempSpark").master("local").getOrCreate()
  }
}