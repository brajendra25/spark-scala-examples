package com.brajendra.LocalToHDFS

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import scala.io.Source

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
  
  def fileHabdling()
  {
      val resourcesPath = getClass.getResource("/scala.txt")
      val _path = Source.getClass.getResource("/scala.txt")
      println(_path.getPath)
      val source = Source.fromURL(getClass.getResource("/scala.txt"))
      try println(source.mkString) finally source.close()
  }
  
}