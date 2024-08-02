import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer

import java.util.Properties
import scala.collection.JavaConverters._

class KafkaConsumerWrapper(bootstrapServers: String, groupId: String, topicName: String) {
  private val consumerProps = new Properties()
  consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
  consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
  consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")

  private val consumer = new KafkaConsumer[String, String](consumerProps)
  @volatile private var running = false

  def startConsuming(): Unit = {
    running = true
    new Thread(() => {
      try {
        while (running) {
            consumer.subscribe(List(topicName).asJava)
            val records = consumer.poll(java.time.Duration.ofMillis(100))
            for (record <- records.asScala) {
              println(s"${record.value()}")
            }
        }
    }catch{
        case _:WakeupException => println("Consumer stopped.")
        case ex: Throwable => println(s"Exception: $ex")
      }
    }).start()
  }

  def stopConsuming(): Unit = {
    running = false
    if (consumer != null) {
      consumer.wakeup()
    }
  }
}
