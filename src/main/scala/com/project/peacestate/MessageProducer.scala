
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer

object MessageProducer {
  def main(args: Array[String]): Unit = {
    val topic = "test-topic"
    val brokers = "localhost:9092"
    val scalaMap: Map[String, String] = Map(
      "bootstrap.servers" -> "localhost:9092",
      "acks" -> "all",
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
    )

    val properties = new Properties()
    scalaMap.foreach { case (k, v) => properties.put(k, v) }

    val producer = new KafkaProducer[String, String](properties)

    val message = "Bon Arnaud"
    val record = new ProducerRecord[String, String](topic, message)

    producer.send(record)
    producer.close()
  }
}