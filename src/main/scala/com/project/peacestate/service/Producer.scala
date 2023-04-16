package com.project.peacestate.service

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import com.project.peacestate.model.DroneReport
import com.project.peacestate.utility.Utils

object Producer {
  def main(arg: Array[String]): Unit = {
    val topic = "drone-report"
    val brokers = "localhost:9092"
    val scalaMap: Map[String, String] = Map(
      "bootstrap.servers" -> "localhost:9092",
      "acks" -> "all",
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
    )
    val properties = new Properties
    scalaMap.foreach(case(k,v) => properties.put(k,v))
    
    val producer = new KafkaProducer[String, String](properties)

    val droneReports = List(
      DroneReport(1, 0.0, 0.0, List(), List()),
      DroneReport(2, 0.0, 0.0, List(), List()),
      DroneReport(3, 0.0, 0.0, List(), List()),
      DroneReport(4, 0.0, 0.0, List(), List()),
      DroneReport(5, 0.0, 0.0, List(), List()),
    )
    val names = List("Ally", "Broussole", "Cody", "Dan", "Enzo", "Fungus", "Gordon", "Hema", "Ian", "Jack")
    val words = List("Knife", "Lower", "Morning", "Nothing", "Operation", "Peace", "Qatar", "Robber", "Silly", "Thanks")

    while (true) {
      droneReports.map(droneReport => Utils.generateReport(droneReport.droneId, names, words))
        .map(Utils.toJson)
        .foreach(droneReport => {
          val record = new ProducerRecord[String, String](topic, droneReport)
          producer.send(record)
        })
      Thread.sleep(3000)
    }
    producer.close()
  }
}