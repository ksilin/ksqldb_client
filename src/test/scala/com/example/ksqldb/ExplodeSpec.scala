package com.example.ksqldb

import io.confluent.ksql.api.client.{
  Client,
  KsqlArray,
  KsqlObject,
  Row,
  StreamedQueryResult
}
import org.apache.kafka.clients.admin.AdminClient
import java.time.Duration
import scala.jdk.CollectionConverters._
import scala.util.Random

class ExplodeSpec extends SpecBase {

  private val setup: LocalSetup        = LocalSetup()
  private val client: Client           = setup.client
  private val adminClient: AdminClient = setup.adminClient
  private val pollTimeout: Duration = Duration.ofMillis(1000)


  override def afterAll(): Unit = {
    client.close()
    super.afterAll()
  }


  "simple explode array" in {
    val topicName = "simpleExplode"
    val streamName         = "preExplodeStream"
    val explodedStreamName = "simpleExplodedStream"

    TestHelper.prepareTest(streamsToDelete = List(streamName, explodedStreamName),
      topicsToCreate = List(topicName),
      client = client, adminClient = adminClient)

    val createArrayStreamSql =
      s"""CREATE STREAM $streamName(
         | id STRING KEY,
         | items ARRAY<STRING>,
         | ts BIGINT)
         | WITH (
         | kafka_topic='$topicName',
         | value_format='json',
         | timestamp = 'ts'
         | );""".stripMargin
      client.executeStatement(createArrayStreamSql).get

    val createExplodeStreamSql =
      s"""CREATE STREAM $explodedStreamName AS SELECT
         | id,
         | EXPLODE(items) AS ITEM,
         | ts
         | FROM $streamName;""".stripMargin
      client.executeStatement(createExplodeStreamSql).get

    (1 to 2) foreach { i =>
      val ksqlObject = makeKsqlObject(i)
      setup.client.insertInto(streamName, ksqlObject).get()
    }

    val querySql = s"""SELECT *
                         |FROM $explodedStreamName
                         |EMIT CHANGES;""".stripMargin
    val q: StreamedQueryResult = client.streamQuery(querySql).get()

    // expecting 8 result rows = 2 source rows with 4 values each
    (1 to 10) foreach { _ =>
      val row: Row = q.poll(pollTimeout)
      println(row)
    }
  }

  "explode and group" in {

    val topicName = "startArrays"

    val streamName         = "startArrayStream"
    val explodedStreamName = "explodedArrayStream"
    val evalStreamName     = "evalStream"

    val collectTableName = "collectArrayTable"
    val splitTable1Name  = "splitTable1"
    val splitTable2Name  = "splitTable2"

    TestHelper.prepareTest(streamsToDelete = List(streamName, explodedStreamName, evalStreamName),
      tablesToDelete = List(collectTableName, splitTable1Name, splitTable2Name),
      topicsToCreate = List(topicName),
      client = client, adminClient = adminClient)

    val createArrayStreamSql =
      s"""CREATE STREAM $streamName(
         | id STRING KEY,
         | items ARRAY<STRING>,
         | ts BIGINT)
         | WITH (
         | kafka_topic='$topicName',
         | value_format='json',
         | timestamp = 'ts'
         | );""".stripMargin
      client.executeStatement(createArrayStreamSql).get

    val createExplodeStreamSql =
      s"""CREATE STREAM $explodedStreamName AS SELECT
         | id,
         | EXPLODE(items) AS ITEM,
         | ts
         | FROM $streamName;""".stripMargin
      client.executeStatement(createExplodeStreamSql).get

    val createEvalStreamSql =
      s"""CREATE STREAM $evalStreamName AS SELECT
         | id,
         | ITEM,
         | LEN(ITEM) AS L,
         | ts
         | FROM $explodedStreamName;""".stripMargin
      client.executeStatement(createEvalStreamSql).get

//    val explodeQuerySql =
//      s"""SELECT id,
//         |ITEM,
//         |LEN(ITEM) AS L
//         |FROM $explodedStreamName
//         |GROUP BY LEN(ITEM)
//         |EMIT CHANGES;""".stripMargin
    // LEN(ITEM) AS L
    // GROUP BY L

//    val explodeQuerySql =
//      s"""SELECT id,
//         |ITEM,
//         |LEN(ITEM) AS L,
//         |HISTOGRAM(LEN(ITEM))
//         |FROM $explodedStreamName
//         |GROUP BY id
//         |EMIT CHANGES;""".stripMargin

    // cannot group directly by L: Received 400 response from server: Line: 5, Col: 10: GROUP BY column 'L' cannot be resolved.
    // Received 400 response from server: Keys missing from projection.

    val collectStreamSql = s"""CREATE TABLE $collectTableName AS
                          |SELECT ID, L,
                          |AS_VALUE(ID) AS IDD,
                          |AS_VALUE(L) AS LL,
                          |LATEST_BY_OFFSET(ts),
                          |COLLECT_LIST(ITEM) AS ITEMS
                          |FROM $evalStreamName
                          |GROUP BY ID, L;""".stripMargin
    client.executeStatement(collectStreamSql).get

    val selectCollectedQuery = s"""SELECT * FROM $collectTableName EMIT CHANGES;""".stripMargin
    val colQ: StreamedQueryResult = client.streamQuery(selectCollectedQuery).get()
    println(s"created query: ${colQ.queryID()}")

    val splitTableLL2Sql = s"""CREATE TABLE $splitTable1Name AS
                              |SELECT *
                              |FROM $collectTableName
                              |WHERE LL = 2;""".stripMargin
    client.executeStatement(splitTableLL2Sql).get

    val splitTableLL6Sql = s"""CREATE TABLE  $splitTable2Name AS
                             |SELECT *
                             |FROM $collectTableName
                             |WHERE LL = 6;""".stripMargin
    client.executeStatement(splitTableLL6Sql).get

    val queryLL2Sql = s"""SELECT *
             |FROM $splitTable1Name
             |EMIT CHANGES;""".stripMargin

    val queryLL6Sql = s"""SELECT *
                         |FROM $splitTable2Name
                         |EMIT CHANGES;""".stripMargin

    val queryLL2: StreamedQueryResult = client.streamQuery(queryLL2Sql).get()
    println(s"created query: ${queryLL2.queryID()}")

    val queryLL6: StreamedQueryResult = client.streamQuery(queryLL6Sql).get()
    println(s"created query: ${queryLL6.queryID()}")

    (1 to 10) foreach { i =>
      val ksqlObject = makeKsqlObject(i)
      setup.client.insertInto(streamName, ksqlObject).get()
    }
    (1 to 10) foreach { _ =>
      val rowFromCollectTable: Row = colQ.poll(pollTimeout)
      println("row CollectTable")
      println(rowFromCollectTable)
      val rowFromSplit2: Row = queryLL2.poll(pollTimeout)
      println("row LL2")
      println(rowFromSplit2)
      val rowFromSplit6: Row = queryLL6.poll(pollTimeout)
      println("row LL6")
      println(rowFromSplit6)
    }
  }

  def makeKsqlObject(idSuffix: Int): KsqlObject = {
    val list = List(
      Random.alphanumeric.take(6).mkString,
      Random.alphanumeric.take(2).mkString,
      Random.alphanumeric.take(2).mkString,
      Random.alphanumeric.take(6).mkString
    )
    new KsqlObject()
      .put("id", "id" + idSuffix.toString)
      .put("items", new KsqlArray(list.asJava))
      .put("ts", System.currentTimeMillis())
  }

}
