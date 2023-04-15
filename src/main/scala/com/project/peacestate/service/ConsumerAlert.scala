package com.project.peacestate.service

import java.util.Properties
import java.util.Colllections.singletonList
import scala.collection.JavaConverters._
import scala.io.AnsiColor._
import org.apache.kafka.common.serialization.StringSerializer
import orf.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import com.project.peacestate.model.Citizen
import com.project.peacestate.utility.Utils

object ConsumerAlert {
  def main(arg: Array[String]): Unit = {
    val topic = "drone-report"
    val brokers = "localhost:9092"
    val scalaMap: Map[String, String] = Map(
      "bootstrap.servers" -> "localhost:9092",
      "group.id" -> "myGroup",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringSerializer"
    )
    val properties = new Properties()
    scalaMap.foreach(case(k,v) => properties.put(k,v))

    val consumer = new KafkaConsumer[String, String](properties)
    consumer.subscribe(singletonList(topic))

    while (true) {
      val records: ConsumerRecords[String, String] = consumer.poll(5)
      records.foreach(record => {
        val droneReport = Utils.fromJson(record.value)
        println(s"Received report: ${record.value}")
        droneReport.citizens.foreach(citizen => citizen match {
          case Citizen(name, peacescore) if peacescore < 30 => println(s"${RED}ALERT: $name's peacescore is lower than 30!!!${RESET}")
          case _ => // do nothing
        })
      })
    }
  }
}