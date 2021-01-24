package com.example.ksqldb

import io.confluent.ksql.api.client.{ Client, KsqlArray, KsqlObject, Row, StreamedQueryResult }
import org.apache.kafka.clients.admin.AdminClient
import java.time.Duration
import scala.jdk.CollectionConverters._
import scala.util.Random

case class Xy(x: Int, y: Int)

class ExplodeSpec extends SpecBase {

  private val setup: LocalSetup        = LocalSetup()
  private val client: Client           = setup.client
  private val adminClient: AdminClient = setup.adminClient
  private val pollTimeout: Duration    = Duration.ofMillis(1000)

  override def afterAll(): Unit = {
    client.close()
    super.afterAll()
  }

  "explode primitive array" in {
    val topicName          = "simpleExplode"
    val streamName         = "preExplodeStream"
    val explodedStreamName = "simpleExplodedStream"

    TestHelper.prepareTest(
      streamsToDelete = List(streamName, explodedStreamName),
      topicsToCreate = List(topicName),
      client = client,
      adminClient = adminClient
    )

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
      val ksqlObject = makeKsqlObjectSingleArray(i)
      setup.client.insertInto(streamName, ksqlObject).get()
    }

    val querySql               = s"""SELECT *
                         |FROM $explodedStreamName
                         |EMIT CHANGES;""".stripMargin
    val q: StreamedQueryResult = client.streamQuery(querySql).get()

    // expecting 8 result rows = 2 source rows with 4 values each
    (1 to 10) foreach { _ =>
      val row: Row = q.poll(pollTimeout)
      println(row)
    }
  }

  "explode struct array" in {
    val topicName          = "structExplode"
    val streamName         = "preStructExplodeStream"
    val explodedStreamName = "structExplodedStream"

    TestHelper.prepareTest(
      streamsToDelete = List(streamName, explodedStreamName),
      topicsToCreate = List(topicName),
      client = client,
      adminClient = adminClient
    )

    val createArrayStreamSql =
      s"""CREATE STREAM $streamName(
         | id STRING KEY,
         | items ARRAY<STRUCT<x INT, y INT>>,
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

    // does not work with Json strings
    // Message: Can't coerce a field of type class io.vertx.core.json.JsonArray (["{\"x\": 1, \"y\": 2}","{\"x\": 3, \"y\": 4}"]) into type ARRAY<STRUCT<`X` INTEGER, `Y` INTEGER>>
    val struct1 = """{"x": 1, "y": 2}"""
    val struct2 = """{"x": 3, "y": 4}"""
    // does nto work with case classes:
    // Row{columnNames=[ID, ITEMS, TS], columnTypes=[ColumnType{type=STRING}, ColumnType{type=ARRAY}, ColumnType{type=BIGINT}],
    // values=["id_struct",[{"X":null,"Y":null},{"X":null,"Y":null}],1611504156948]}
    val xy1 = Xy(1, 2)
    val xy2 = Xy(3, 4)

    // Scala Maps do not work
    //  Row{columnNames=[ID, ITEMS, TS], columnTypes=[ColumnType{type=STRING}, ColumnType{type=ARRAY}, ColumnType{type=BIGINT}],
    //  values=["id_struct",[{"X":null,"Y":null},{"X":null,"Y":null}],1611504310226]}

    // Java maps work
    val xyMap1 = Map("x" -> 1, "y" -> 2).asJava
    val xyMap2 = Map("x" -> 3, "y" -> 4).asJava

    val o = new KsqlObject()
      .put("id", "id_struct")
      .put("items", new KsqlArray(List(xyMap1, xyMap2).asJava))
      .put("ts", System.currentTimeMillis())
    setup.client.insertInto(streamName, o).get()

    val querySql               = s"""SELECT *
                                    |FROM $explodedStreamName
                                    |EMIT CHANGES;""".stripMargin
    val q: StreamedQueryResult = client.streamQuery(querySql).get()

    // expecting 2 elements
    (1 to 3) foreach { _ =>
      val row: Row = q.poll(pollTimeout)
      println(row)
    }
  }

  // https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-reference/table-functions/#explode
  // You can also use multiple table functions in a SELECT clause.
  // In this situation, the results of the table functions are "zipped" together.
  // The total number of rows returned is equal to the greatest number of values returned from any of the table functions.
  // If some of the functions return fewer rows than others, the missing values are replaced with null
  "explode multiple arrays" in {
    val topicName          = "multiExplode"
    val streamName         = "preMultiExplodeStream"
    val explodedStreamName = "multiExplodedStream"

    TestHelper.prepareTest(
      streamsToDelete = List(streamName, explodedStreamName),
      topicsToCreate = List(topicName),
      client = client,
      adminClient = adminClient
    )

    val createArrayStreamSql =
      s"""CREATE STREAM $streamName(
         | id STRING KEY,
         | items ARRAY<STRING>,
         | numbers ARRAY<INT>,
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
         | EXPLODE(numbers) AS NUM,
         | ts
         | FROM $streamName;""".stripMargin
    client.executeStatement(createExplodeStreamSql).get

    (1 to 2) foreach { i =>
      val ksqlObject = makeKsqlObjectTwoArrays(i)
      setup.client.insertInto(streamName, ksqlObject).get()
    }

    val querySql               = s"""SELECT *
                      |FROM $explodedStreamName
                      |EMIT CHANGES;""".stripMargin
    val q: StreamedQueryResult = client.streamQuery(querySql).get()

    // expecting 8 result rows = 2 source rows with 4 values in the longest array
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

    TestHelper.prepareTest(
      streamsToDelete = List(streamName, explodedStreamName, evalStreamName),
      tablesToDelete = List(collectTableName, splitTable1Name, splitTable2Name),
      topicsToCreate = List(topicName),
      client = client,
      adminClient = adminClient
    )

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

    val selectCollectedQuery      = s"""SELECT * FROM $collectTableName EMIT CHANGES;""".stripMargin
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
      val ksqlObject = makeKsqlObjectSingleArray(i)
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

  def makeKsqlObjectSingleArray(idSuffix: Int): KsqlObject = {

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

  def makeKsqlObjectTwoArrays(idSuffix: Int): KsqlObject = {
    val list = List(
      Random.nextInt(100),
      Random.nextInt(100),
      Random.nextInt(100)
    )
    makeKsqlObjectSingleArray(idSuffix).put("numbers", new KsqlArray(list.asJava))
  }

}
