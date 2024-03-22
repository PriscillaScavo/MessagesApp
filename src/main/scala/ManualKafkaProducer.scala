import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import scala.io.StdIn

object MessageProducer extends App {
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  try {
    while (true) {
      println("Inserisci il tuo messaggio (digita 'exit' per uscire):")
      val message = StdIn.readLine()


      if (message.toLowerCase == "exit") {
        println("Uscita...")
        System.exit(0)
      }

      val record = new ProducerRecord[String, String]("chat-messages", message)
      producer.send(record)
      println("Messaggio inviato con successo.")
    }
  } finally {
    producer.close()
  }
}
