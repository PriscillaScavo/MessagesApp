object ChatApp extends App {
  val bootstrapServers = "localhost:9092"
  val partitions = 3
  val replicationFactor = 1

  println("Would you like to delete existing topics? y/n")
  val response = scala.io.StdIn.readLine()
  if(response.equals("y"))
    KafkaTopicUtils.deleteAllTopics(bootstrapServers)

  var exceptionThrown = false
  var topicName = ""
  var groupId = ""
  println("Choose your username")
  do {
    try{
      exceptionThrown = false
      topicName = scala.io.StdIn.readLine()
      groupId = topicName
      KafkaTopicUtils.createTopic(bootstrapServers, topicName, partitions, replicationFactor)
    }catch
    {
      case ex: Throwable => println(s"exception: $ex")
        exceptionThrown = true
        println("Choose another username")
    }
  }while(exceptionThrown)

  val consumer = new KafkaConsumerWrapper(bootstrapServers, groupId, topicName)
  consumer.startConsuming()

  val producer = new KafkaProducerWrapper(bootstrapServers)

  // send a message
  var messagePrinted = false

  while (true) {
    val topics = KafkaTopicUtils.getTopics(bootstrapServers).filter(!_.equals(topicName))
    if(topics.isEmpty){
      if(!messagePrinted) {
        println("There are no other users to send a message to")
        messagePrinted = true
      }
    }
    else{
      messagePrinted = false
      println("Do you want to stop the consumer? y/n")
      val stopResponse = scala.io.StdIn.readLine()
      if (stopResponse.equals("y")) {
        consumer.stopConsuming()
        var restart = true
        while(restart){
          println("Do you want to restart the consumer? y/n")
          val restartResponse = scala.io.StdIn.readLine()
          if(restartResponse.equals("y")) {
            restart = false
            consumer.startConsuming()
            println("Consumer restarted.")
          }
        }

      }
      println(s"$topicName, choose a user to send a message between: ${topics.filter(!_.equals(topicName)).toString()}")
      val recipient = scala.io.StdIn.readLine()
      if(topics.contains(recipient) && recipient.nonEmpty) {
        println(s"write you message for $recipient")
        val message = scala.io.StdIn.readLine()
        producer.sendMessage(recipient, topicName, message)
      }
      else
        println("this username doesn't exist")
    }
  }
}
