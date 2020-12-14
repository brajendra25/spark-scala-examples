package com.brajendra.entry

import com.brajendra.DemoSparkSession
import com.brajendra.LocalToHDFS
import com.brajendra.LocalToHDFS.CopyFile

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext




object MainEntry {
  def main(args: Array[String]): Unit = {
    println("Start Exceution :")
    val _spark = DemoSparkSession._sparkSession()
    println("**********Calling Demo Function***************")
    //this.demo(_spark);
    //this.Spark_RDD()
    this.SQLQueries()
   }
  
    def demo(_spark:SparkSession )  {
          
      import _spark.implicits._
     // Employee Data
      val empDF = Seq((8, "John" , 1),(64, "Mike", 2), (27, "Garner", 1)).toDF("EmpId", "EmpName" , "DepId")
      empDF.show()
      
      // Department Data
      val depDF = Seq((1,"IT"),(2,"ACCOUNTS")).toDF("DepId" , "DepName")
      depDF.show()
      
      // Joined Data
      val resultant = empDF.join(depDF, "DepId").select($"EmpName", $"DepName")
      resultant.show()
      
      println("Finishing the entry point --------------->")
  }
    
  def Spark_RDD()
   {
     println("******RDD Examples************")
     CopyFile.ReadCSV()
    }
  def SQLQueries()
  {
    println("******Sql Queries Examples************")
     CopyFile.SQLQueries()
  }
  
    
}
   


