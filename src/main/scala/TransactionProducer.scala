package de.phinguyen.sparkstructuredstreaming

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.time.Instant
import java.util.{Properties, UUID}
import scala.util.Random

object TransactionProducer {

  def main(args: Array[String]) = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](props)

    val topic = "user-transaction-topic"
    val userIds = (1L to 10000L).toList // simulate 10000 users

    while (true) {
      // Generate a random transaction for a random user
      val userId = userIds(Random.nextInt(userIds.length))
      val transactionTimestamp = Instant.now().toString

      // Create a JSON-like string for the transaction record
      val value = s"""{"userId": "$userId", "transactionTimestamp": "$transactionTimestamp"}"""

      // Send the record to Kafka
      val record = new ProducerRecord[String, String](topic, userId.toString, value)
      producer.send(record)

      println(s"Produced record to $topic: $value")

      // Sleep for a random duration to simulate real-time transaction data
      Thread.sleep(Random.nextInt(100) + 50)
    }
    // Close the producer (in practice, you might want to add a shutdown hook)
    // producer.close()
  }
}
