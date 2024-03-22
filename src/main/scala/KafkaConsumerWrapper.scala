import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import java.util.Properties
import scala.collection.JavaConverters._

class KafkaConsumerWrapper(bootstrapServers: String, groupId: String, topicName: String) {
  private val consumerProps = new Properties()
  consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
  consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)

  private val consumer = new KafkaConsumer[String, String](consumerProps)

  def startConsuming(): Unit = {
    new Thread(() => {
      try {
        while (true) {
            consumer.subscribe(List(topicName).asJava)
            val records = consumer.poll(java.time.Duration.ofMillis(100))
            for (record <- records.asScala) {
              println(s"${record.value()}")
            }
        }
    }catch{
        case ex: Throwable => println(s"Exception: $ex")
      }finally{
      consumer.close()
    }
    }).start()
  }
}
