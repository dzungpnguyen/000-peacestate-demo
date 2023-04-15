package com.project.peacestate.service

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
// import org.apache.spark.sql.{DataFrame, SparkSession}
import com.project.peacestate.model.{DroneReport, Citizen}
import com.project.peacestate.utility.Utils

object Analysis {
  def main(arg: Array[String]): Unit = {
    val conf = new Configuration
    val fs = FileSystem.get(conf)
    val hdfsUri = "hdfs://localhost:8020/drone-report/reports.json"
    val storagePath = new Path(hdfsUri)

    val reports = loadData(hdfsUri)
  }

  def loadData(hdfsUri: String): RDD[DroneReport] = {
    val conf = new SparkConf()
      .setAppName("drone-report-analysis")
      .setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    sc.textFile(hdfsUri)
      .map(Utils.fromJson)
      // ... ??
  }
}