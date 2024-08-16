package de.phinguyen.sparkstructuredstreaming

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

  


}
