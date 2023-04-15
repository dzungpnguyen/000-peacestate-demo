package com.project.peacestate.service

import java.util.Properties
import java.util.Collections.singletonList
import scala.collection.JavaConverters._
import scala.io.AnsiColor._
import orf.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, FSDataOutputStream}

object ConsumerStorage {
  def main(arg: Array[String]): Unit = {
    val topic = "drone-report"
    val brokers = "localhost:9092"
    val scalaMap: Map[String, String] = Map(
      "bootstrap.servers" -> "localhost:9092",
      "group.id" -> "myGroup",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )
    val properties = new Properties
    scalaMap.foreach(case(k,v) => properties.put(k,v))

    val consumer = new KafkaConsumer[String, String](properties)
    consumer.subscribe(singletonList(topic))

    val conf = new Configuration
    val hdfsUri = "hdfs://localhost:8020"
    conf.set("fs.defaultFS", hdfsUri)
    val fs = FileSystem.get(conf)

    while (true) {
      val records: ConsumerRecords[String, String] = consumer.poll(5)
      records.foreach(record => {
        val storagePath = new Path("/drone-report/reports.json")
        val output: FSDataOutputStream = fs.create(path, true)
        // output.write(record.value.getBytes)
        output.write(record.value.getBytes("UTF-8"))
        output.write("\n".getBytes("UTF-8"))
        output.close()
      })
    }
  }
}