package com.brajendra.LocalToHDFS
import com.brajendra.DemoSparkSession

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import scala.io.Source
import scala.util.control.Exception.Finally



object CopyFile {
  
  def main(args: Array[String]): Unit = {
   this.copy_from_local_to_hdfs()
  }
  //Working  fine Code
  //Please start first all demons hdfs and yarn
  def copy_from_local_to_hdfs() : Boolean =
  {
      println( "Start to write into HDFS..." )
      val conf = new Configuration()
      //conf.set("fs.defaultFS", "hdfs://127.0.0.1:9000/brajendra")
      val _cmd = "fs.defaultFS";
      val hadoopPath = "hdfs://127.0.0.1:9000/"
      conf.set(_cmd, hadoopPath)
      val fs= FileSystem.get(conf)
      
      val _path = Source.getClass.getResource("/scala.txt")
      //val output = fs.create(new Path(resourcesPath.getPath))
      fs.copyFromLocalFile(new Path(_path.getPath), new Path(hadoopPath + "/brajendra/"))
      println("Done!")
      return true;
   }
  
  def ReadCSV()
  {
     val _spark = DemoSparkSession._sparkSession()
    import  _spark.implicits._
     val _path = Source.getClass.getResource("/CountryWise_Summary.csv")
     println(_path)
     val dfCSV = _spark.read.option("header", true).csv(_path.getPath)
     println("*******Showing Results********")
     dfCSV.show()
     println("******Schema of DataFrame******")
     dfCSV.printSchema()
     
     println("Print Country Column Values")
     dfCSV.select("Country","NewConfirmed").show()
     dfCSV.filter($"NewConfirmed" > 20).show()
     dfCSV.groupBy("Slug").count().show()
     _spark.close()
     //return dfCSV
  }
  //*****Running SQL Queries Programmatically***
  def SQLQueries()
  {
    val _spark = DemoSparkSession._sparkSession()
    try
      {
        import  _spark.implicits._
        val _path = Source.getClass.getResource("/CountryWise_Summary.csv")
        val dfCSV = _spark.read.option("header", true).csv(_path.getPath)
        
        //Register the DataFrame as a SQL temporary view
        dfCSV.createTempView("Country")
        val sqlDf = _spark.sql("select *  from Country Limit 5")
        sqlDf.show()
        
        /*println("Temp View, New Session " + _spark.newSession()) 
        val sqlNdf = _spark.newSession().sql("select * from Country")
        sqlNdf.show()*/
        
        
        // Register the DataFrame as a global temporary view
        dfCSV.createGlobalTempView("Country")
        val sqlGdf = _spark.sql("select * from global_temp.Country")
        println("***** In Same Session*****")
        println("Current Session " + _spark)
        sqlGdf.show()
        
        println("****In Cross-session ****")
        println("Global Temp View , New Session " + _spark.newSession()) 
        val sqlCdf = _spark.newSession().sql("select * from global_temp.Country")
        sqlCdf.show()
      
      }
    finally {
      _spark.close()
      
    }
    
  }
  
}