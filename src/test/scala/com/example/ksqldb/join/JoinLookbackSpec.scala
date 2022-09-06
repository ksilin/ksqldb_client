package com.example.ksqldb.join

import com.example.ksqldb.JsonStringProducer
import com.example.ksqldb.util.{ KafkaSpecHelper, KsqlSpecHelper, SpecBase }
import io.circe.generic.auto._
import io.confluent.ksql.api.client.{ Row, StreamedQueryResult }

import java.time.Duration
import scala.concurrent.duration._
import scala.jdk.DurationConverters._
import scala.util.Random

class JoinLookbackSpec extends SpecBase(configPath = Some("ccloud.ps.ksilin.dedicated_ksilin")) {

  case class MyRecord(id: String, value: String, ts: Long)

  val prefix: String  = suiteName
  val leftTopicName   = s"${prefix}_left"
  val rightTopicName  = s"${prefix}_right"
  val leftStreamName  = s"${prefix}_leftStream"
  val rightStreamName = s"${prefix}_rightStream"
  val leftTableName   = s"${prefix}_leftTable"
  val rightTableName  = s"${prefix}_rightTable"

  val joinResultName = s"${prefix}_joinResult"

  // produce test data
  val leftProducer: JsonStringProducer[String, MyRecord] = JsonStringProducer[String, MyRecord](
    setup.commonProps,
    leftTopicName,
    "leftProducer" + Random.alphanumeric.take(3).mkString
  )
  val rightProducer: JsonStringProducer[String, MyRecord] = JsonStringProducer[String, MyRecord](
    setup.commonProps,
    rightTopicName,
    "rightProducer" + Random.alphanumeric.take(3).mkString
  )

  val createLeftStreamSql: String = createSourceSql(leftStreamName, leftTopicName, "STREAM")
  val createRightStreamSql: String = createSourceSql(rightStreamName, rightTopicName, "STREAM")

  val createLeftTableSql: String = createSourceSql(leftTableName, leftTopicName, "TABLE")

  val createRightTableSql: String =  createSourceSql(rightTableName, rightTopicName, "TABLE")

  // DO we need PRIMARY for tables?
  def createSourceSql(sourceName: String, topicName: String, sourceType: String): String =
    s"""CREATE $sourceType $sourceName (
         | id STRING ${if ("TABLE" == sourceType) "PRIMARY" else ""} KEY,
         | value STRING,
         | ts BIGINT
         | ) WITH (
         | KAFKA_TOPIC = '$topicName',
         | VALUE_FORMAT = 'JSON',
         | TIMESTAMP = 'ts'
         | );""".stripMargin

  def createJoinStreamSql(
      resultName: String,
      leftSide: String,
      rightSide: String,
      windowed: Boolean = true,
      joinType: String = "INNER",
      resultType: String = "STREAM"
  ): String = {
    val window = "WITHIN 100 DAYS"
    s"""CREATE $resultType $resultName AS SELECT
       | L.id,
       | L.value AS LVAL,
       | L.ts AS LTS,
       | R.value AS RVAL,
       | R.ts AS RTS
       | FROM $leftSide AS L
       | $joinType JOIN $rightSide AS R
       | ${if (windowed) window else ""}
       | ON L.id = R.id
       | ;""".stripMargin
  }

  override def afterAll(): Unit = {
    ksqlClient.close()
    super.afterAll()
  }

  "stream to stream inner join - new entries trigger join from both sides, full lookback" in {

    prepareStreams()

    ksqlClient.executeStatement(createLeftStreamSql, queryProperties).get
    ksqlClient.executeStatement(createRightStreamSql, queryProperties).get

    ksqlClient
      .executeStatement(
        createJoinStreamSql(joinResultName, leftStreamName, rightStreamName),
        queryProperties
      )
      .get

    val joinedQuerySql             = s"SELECT * FROM $joinResultName EMIT CHANGES;"
    val query: StreamedQueryResult = ksqlClient.streamQuery(joinedQuerySql, queryProperties).get

    produceRecordsFetchJoinResults(query)
  }

  "stream-table inner join - left side triggers join, no lookback" in {

    prepareStreams()
    ksqlClient.executeStatement(createLeftStreamSql, queryProperties).get
    ksqlClient.executeStatement(createRightTableSql, queryProperties).get

    ksqlClient
      .executeStatement(
        createJoinStreamSql(joinResultName, leftStreamName, rightTableName, windowed = false),
        queryProperties
      )
      .get

    val joinedQuerySql             = s"SELECT * FROM $joinResultName EMIT CHANGES;"
    val query: StreamedQueryResult = ksqlClient.streamQuery(joinedQuerySql, queryProperties).get

    produceRecordsFetchJoinResults(query)
  }

  "table-table inner join, both sides trigger join, no lookback" in {

    prepareStreams()
    ksqlClient.executeStatement(createLeftTableSql, queryProperties).get
    ksqlClient.executeStatement(createRightTableSql, queryProperties).get

    ksqlClient
      .executeStatement(
        createJoinStreamSql(
          joinResultName,
          leftTableName,
          rightTableName,
          windowed = false,
          resultType = "TABLE"
        ),
        queryProperties
      )
      .get

    val joinedQuerySql             = s"SELECT * FROM $joinResultName EMIT CHANGES;"
    val query: StreamedQueryResult = ksqlClient.streamQuery(joinedQuerySql, queryProperties).get

    produceRecordsFetchJoinResults(query)
  }

  private def produceRecordsFetchJoinResults(query: StreamedQueryResult) = {

    val now = System.currentTimeMillis()

    val left1 = MyRecord(id = "1", value = "A", ts = now - Duration.ofMinutes(10).toMillis)
    val left2 = MyRecord(id = "1", value = "B", ts = now - Duration.ofMinutes(8).toMillis)
    val left4 = MyRecord(id = "1", value = "C", ts = now - Duration.ofMinutes(6).toMillis)
    val left6 = MyRecord(id = "1", value = "D", ts = now - Duration.ofMinutes(2).toMillis)

    val right1 = MyRecord(id = "1", value = "a", ts = now - Duration.ofMinutes(9).toMillis)
    val right2 = MyRecord(id = "1", value = "b", ts = now - Duration.ofMinutes(7).toMillis)
    val right4 = MyRecord(id = "1", value = "c", ts = now - Duration.ofMinutes(5).toMillis)
    val right6 = MyRecord(id = "1", value = "d", ts = now - Duration.ofMinutes(3).toMillis)

    val leftRecord1 =
      leftProducer.makeRecords(Map(left1.id -> left1))
    val leftRecord2 =
      leftProducer.makeRecords(Map(left2.id -> left2))
    val leftRecord3 =
      leftProducer.makeRecords(Map("1" -> null))
    val leftRecord4 =
      leftProducer.makeRecords(Map(left4.id -> left4))
    val leftRecord5 =
      leftProducer.makeRecords(Map("1" -> null))
    val leftRecord6 =
      leftProducer.makeRecords(Map(left6.id -> left6))

    val rightRecord1 =
      rightProducer.makeRecords(Map(right1.id -> right1))
    val rightRecord2 =
      rightProducer.makeRecords(Map(right2.id -> right2))
    val rightRecord3 =
      rightProducer.makeRecords(Map("1" -> null))
    val rightRecord4 =
      rightProducer.makeRecords(Map(right4.id -> right4))
    val rightRecord5 =
      rightProducer.makeRecords(Map("1" -> null))
    val rightRecord6 =
      rightProducer.makeRecords(Map(right6.id -> right6))

    info("produce first record left")
    leftProducer.run(leftRecord1)
    fetchAvailableRows(query) // foreach println

    info("produce first record right")
    rightProducer.run(rightRecord1)
    fetchAvailableRows(query) // foreach println

    info("produce second record left")
    leftProducer.run(leftRecord2)
    fetchAvailableRows(query) // foreach println

    info("produce second record right")
    rightProducer.run(rightRecord2)
    fetchAvailableRows(query) // foreach println

    info("produce third record left - tombstone")
    leftProducer.run(leftRecord3)
    fetchAvailableRows(query) // foreach println

    info("produce third record right - tombstone")
    rightProducer.run(rightRecord3)
    fetchAvailableRows(query) // foreach println

    info("produce fourth record left")
    leftProducer.run(leftRecord4)
    fetchAvailableRows(query) // foreach println

    info("produce fourth record right")
    rightProducer.run(rightRecord4)
    fetchAvailableRows(query) // foreach println

    //

    info("produce fifth record right - tombstone")
    rightProducer.run(rightRecord5)
    fetchAvailableRows(query) // foreach println

    info("produce fifth record left - tombstone")
    leftProducer.run(leftRecord5)
    fetchAvailableRows(query) // foreach println

    info("produce sixth record right")
    rightProducer.run(rightRecord6)
    fetchAvailableRows(query) // foreach println

    info("produce sixth record left")
    leftProducer.run(leftRecord6)
    fetchAvailableRows(query, 10000.millis.toJava) // foreach println
  }

  def fetchAvailableRows(
      query: StreamedQueryResult,
      fetchTimeout: Duration = 1000.millis.toJava
  ): List[Row] = {

    var row: Row        = query.poll(fetchTimeout)
    var rows: List[Row] = Nil
    while (row != null) {
      println(row.values())
      rows = row :: rows
      row = query.poll(fetchTimeout)
    }
    rows
  }

  def prepareStreams(): Unit = {

    KsqlSpecHelper.deleteTable(joinResultName, ksqlClient)
    KsqlSpecHelper.deleteStream(joinResultName, ksqlClient, adminClient)

    KsqlSpecHelper.deleteTable(leftTableName, ksqlClient)
    KsqlSpecHelper.deleteTable(rightTableName, ksqlClient)

    KsqlSpecHelper.deleteStream(leftStreamName, ksqlClient, adminClient)
    KsqlSpecHelper.deleteStream(rightStreamName, ksqlClient, adminClient)

    KafkaSpecHelper.deleteTopic(adminClient, leftTopicName)
    KafkaSpecHelper.deleteTopic(adminClient, rightTopicName)
    KafkaSpecHelper.createOrTruncateTopic(
      adminClient,
      leftTopicName,
      replicationFactor = replicationFactor
    )
    KafkaSpecHelper.createOrTruncateTopic(
      adminClient,
      rightTopicName,
      replicationFactor = replicationFactor
    )
  }

}
