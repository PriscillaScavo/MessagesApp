import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, ListTopicsOptions, NewTopic}
import java.util.Properties
import scala.collection.JavaConverters._

object KafkaTopicUtils {
  val adminProps = new Properties()

  def getTopics(bootstrapServers: String): List[String] = {
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    val adminClient = AdminClient.create(adminProps)
    val listTopicsOptions = new ListTopicsOptions().listInternal(false)
    val topics = adminClient.listTopics(listTopicsOptions).names().get().asScala.toList
    adminClient.close()
    topics
  }

  def createTopic(bootstrapServers: String, topicName: String, partitions: Int, replicationFactor: Int): Unit = {
      adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      val adminClient = AdminClient.create(adminProps)

      val newTopic = new NewTopic(topicName, partitions, replicationFactor.toShort)
      adminClient.createTopics(List(newTopic).asJava).all().get()

      adminClient.close()
  }

  def deleteAllTopics(bootstrapServers: String): Unit = {
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    val adminClient = AdminClient.create(adminProps)

    val topicsToDelete = adminClient.listTopics().names().get().asScala.toList
    adminClient.deleteTopics(topicsToDelete.asJava).all().get()

    adminClient.close()
  }
}
