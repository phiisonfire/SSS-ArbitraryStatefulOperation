package de.phinguyen.sparkstructuredstreaming

import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}

import java.time.{Duration, Instant}
import scala.collection.mutable.ListBuffer

object Main {
  /* Transaction count within the last 5 minutes

  * When a transaction record is received for a user, the count of
  * transactions for that user that occurred within the last 5 minutes of the
  * transaction time is calculated and written to a table.
  *
  * Only the current count for a given user is kept. If no transactions are
  * received for a user (after a period of time), the count should automatically go down -> update the table. An ML model
  * uses the count to determine if too many have occurred within the last 5 minutes.
  * */

  // Define data structures - what goes in, what is stored (in state, between micro-batches), what comes out (emit).
  case class InputRow(userId: Long,
                      transactionTimestamp: java.time.Instant)
  case class PurchaseCountState(latestTimeStamp: java.time.Instant,
                                currentPurchases: List[InputRow])
  case class PurchaseCount(userId: Long,
                           purchaseCount: Integer,
                           eventTimestamp: java.time.Instant,
                           isTimeout: Boolean)

  // Support functions
  def addNewRecords(transactionList: List[InputRow], state: PurchaseCountState) = {
    val latestTimestamp = if (transactionList.nonEmpty) {
      transactionList.map(row => row.transactionTimestamp).max
    } else {
      state.latestTimeStamp
    }
    new PurchaseCountState(latestTimestamp, state.currentPurchases ::: transactionList)
  }

  def removeExpiredRecords(latestTimestamp: java.time.Instant,state: PurchaseCountState) = {
    // val latestTimestamp = state.latestTimeStamp
    val expirationDuration = Duration.ofMinutes(1)
    val filterPurchases = state.currentPurchases.filter( purchase =>
      purchase.transactionTimestamp.isAfter(latestTimestamp.minus(expirationDuration))
    )
    new PurchaseCountState(latestTimestamp, filterPurchases)
  }

  // Function (with a specific set of parameters) that will be executed by `flatMapGroupWithState`
  // 1st parameter ~ grouping key = the column from the streaming DataFrame being group on

  def updateState(userId: Long,
                  values: Iterator[InputRow],
                  state: GroupState[PurchaseCountState]
                 ): Iterator[PurchaseCount] = {
    val purchaseCounts = ListBuffer[PurchaseCount]()
    if (!state.hasTimedOut) { // login when new records received within the timeout period
      val transactionList = new ListBuffer[InputRow]()
      transactionList ++= values
      var prevState = state.getOption.getOrElse {
        // initialization logic - when received the first record of the key
        val firstTransactionTimestamp = transactionList.head.transactionTimestamp
        new PurchaseCountState(firstTransactionTimestamp, List[InputRow]())
      }
      // steady-state logic - when received new records for existing key
      val stateWithNewRecords = addNewRecords(transactionList.toList, prevState)
      val stateWithRecordsRemoved = removeExpiredRecords(stateWithNewRecords.latestTimeStamp, stateWithNewRecords)
      val output = new PurchaseCount(userId,
        stateWithRecordsRemoved.currentPurchases.size,
        stateWithRecordsRemoved.latestTimeStamp,
        false
      )
      purchaseCounts.append(output)
      state.update(stateWithRecordsRemoved)
      // reset the timeout by extending 30 seconds from the latest event timestamp
      state.setTimeoutTimestamp(stateWithRecordsRemoved.latestTimeStamp.toEpochMilli(), "30 seconds")
    } else { // logic when no events received within the timeout period
      // Timeout logic will not have an incoming set of new records, so no need to access the input Iterator
      val prevState = state.get
      val newTimestamp = Instant.now
      val stateWithRecordsRemoved = removeExpiredRecords(newTimestamp, prevState)
      val output = new PurchaseCount(userId = userId,
        purchaseCount = stateWithRecordsRemoved.currentPurchases.size,
        eventTimestamp = stateWithRecordsRemoved.latestTimeStamp,
        isTimeout = true)
      purchaseCounts.append(output)
      state.update(stateWithRecordsRemoved)
      state.setTimeoutTimestamp(stateWithRecordsRemoved.latestTimeStamp.toEpochMilli(), "30 seconds")
    }
    purchaseCounts.iterator
    // time-out logic - when no records have been received for a key (for a period of time)
    // When we need a time out?
    // - If results need to be output or the state needs to change even if no records have been received
  }

  def main(args: Array[String]) = {

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("SSS-ArbitraryStatefulProcessing")
      .getOrCreate()

    // Import implicits for Encoders
    // Spark automatically creates Encoder for case classes
    // in this situation, Spark creates Encoder[InputRow]
    // Encoders are necessary because they define how the JVM objects are serialized and deserialzed when working with DataFrames and DataSets
    import spark.implicits._

    val kafkaRawInputDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "user-transaction-topic")
      .option("startingOffsets", "earliest")
      .option("group.id")
      .load() // this load the whole kafka messages and their metadata

    // Define the schema for parsing the value of messages (JSON String)
    val schema = StructType(Seq(
      StructField("userId", LongType, true),
      StructField("transactionTimestamp", StringType, true)
    ))

    val kafkaParsedInputDS = kafkaRawInputDF
      .selectExpr("CAST(value AS STRING) as value")
      .select(from_json(col("value"), schema).as("json_data"))
      .select("json_data.userId", "json_data.transactionTimestamp")
      .withColumn("transactionTimestamp", col("transactionTimestamp").cast(TimestampType))
      .as[InputRow]

    val resultDS: Dataset[PurchaseCount] = kafkaParsedInputDS
      .withWatermark("transactionTimestamp", "30 seconds")
      .groupByKey(_.userId)
      .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.EventTimeTimeout())(updateState)

    val csvOutputFilePath = "./data/streaming-outputs"
    val checkpointPath = "./data/checkpoints"
    // recommend convert resultDS to DataFrame
    // sink: Kafka, Delta, foreachBatch
    resultDS.writeStream
      .format("csv")
      .option("path", csvOutputFilePath)
      .option("checkpointLocation", checkpointPath)
      .option("header", "true")
      .outputMode("append")
      .start()
      .awaitTermination()

  }













}
