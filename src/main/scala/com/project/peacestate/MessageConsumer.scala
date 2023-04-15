import java.util.Properties

import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import scala.collection.JavaConverters._

object MessageConsumer {
  def main(args: Array[String]): Unit = {
    val topic = "test-topic"
    val brokers = "localhost:9092"

    val scalaMap: Map[String, String] = Map(
      "bootstrap.servers" -> "localhost:9092",
      "group.id" -> "myGroup",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )


    val properties = new Properties()
    scalaMap.foreach { case (k, v) => properties.put(k, v) }

    val consumer = new KafkaConsumer[String, String](properties)

    consumer.subscribe(java.util.Collections.singletonList(topic))
    while (true) {
      val records: ConsumerRecords[String, String] = consumer.poll(100)
      records.forEach(record => {
        println(s"Received message: ${record.value}")
      })
    }



  }
}

