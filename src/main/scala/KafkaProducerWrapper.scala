import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

class KafkaProducerWrapper(bootstrapServers: String) {
  private val producerProps = new Properties()
  producerProps.put("bootstrap.servers", bootstrapServers)
  producerProps.put("key.serializer", classOf[StringSerializer].getName)
  producerProps.put("value.serializer", classOf[StringSerializer].getName)

  private val producer = new KafkaProducer[String, String](producerProps)

  def sendMessage(topic: String, topicName:String, message: String): Unit = {
    val contentMessage = s"New message from $topicName: $message"
    val record = new ProducerRecord[String, String](topic, contentMessage)
    producer.send(record)
    println("Message succeed sent.")
  }
}
