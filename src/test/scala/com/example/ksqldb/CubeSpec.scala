package com.example.ksqldb

import io.confluent.ksql.api.client._
import io.confluent.ksql.api.client.exception.KsqlClientException
import org.apache.kafka.clients.admin.AdminClient

import java.time.Duration
import java.util.concurrent.ExecutionException
import scala.jdk.CollectionConverters._
import scala.util.Random

class CubeSpec extends SpecBase {

  private val setup: LocalSetup        = LocalSetup()
  private val client: Client           = setup.client
  private val adminClient: AdminClient = setup.adminClient
  private val pollTimeout: Duration    = Duration.ofMillis(1000)

  override def afterAll(): Unit = {
    client.close()
    super.afterAll()
  }

  // we do not explain the cube function: https://confluentinc.atlassian.net/browse/DOCS-4528

  // https://github.com/confluentinc/ksql/blob/master/ksqldb-engine/src/main/java/io/confluent/ksql/function/udtf/Cube.java

  // existing tests do not cover combining different types
  // https://github.com/confluentinc/ksql/blob/0.14.0-ksqldb/ksqldb-functional-tests/src/test/resources/query-validation-tests/cube.json

  // casting within CUBE_EXPLODE is not supported
  // Received 400 response from server: line 3:30: no viable alternative at input 'array[ CAST(num)'. Error code: 40001
  "cube fails with scalar fields of different types" in {
    val topicName       = "scalarCubeSource"
    val streamName      = "scalarCubeSourceStream"
    val cubedStreamName = "scalarCubeTargetStream"

    TestHelper.prepareTest(
      streamsToDelete = List(streamName, cubedStreamName),
      topicsToCreate = List(topicName),
      client = client,
      adminClient = adminClient
    )

    val createArrayStreamSql =
      s"""CREATE STREAM $streamName(
         | id STRING KEY,
         | item STRING,
         | num INT,
         | ts BIGINT)
         | WITH (
         | kafka_topic='$topicName',
         | value_format='json',
         | timestamp = 'ts'
         | );""".stripMargin
    client.executeStatement(createArrayStreamSql).get

    // no casting possible in CUBE_EXPLODE - cube_explode(array[ CAST(num) AS STRING, item]) AS COMBO
    // cube_explode(array[ CAST(num) AS STRING, item]) AS COMBO,
    // Received 400 response from server: line 3:30: no viable alternative at input 'array[ CAST(num)'. Error code: 40001

    val createCubeStreamSql =
      s"""CREATE STREAM $cubedStreamName AS SELECT
         | id,
         | cube_explode(array[num, item]) AS COMBO,
         | ts
         | FROM $streamName;""".stripMargin

    val ex: ExecutionException = intercept[ExecutionException] {
      client.executeStatement(createCubeStreamSql).get
    }
    ex.getCause mustBe a[KsqlClientException]
    ex.getMessage.contains(
      "Cannot construct an array with mismatching types ([INTEGER, STRING]) from expression ARRAY[NUM, ITEM]"
    ) mustBe true
  }

  "cube with scalar string fields" in {
    val topicName       = "scalarCubeSource"
    val streamName      = "scalarCubeSourceStream"
    val cubedStreamName = "scalarCubeTargetStream"

    TestHelper.prepareTest(
      streamsToDelete = List(streamName, cubedStreamName),
      topicsToCreate = List(topicName),
      client = client,
      adminClient = adminClient
    )

    val createArrayStreamSql =
      s"""CREATE STREAM $streamName(
         | id STRING KEY,
         | item STRING,
         | name STRING,
         | ts BIGINT)
         | WITH (
         | kafka_topic='$topicName',
         | value_format='json',
         | timestamp = 'ts'
         | );""".stripMargin
    client.executeStatement(createArrayStreamSql).get

    // no casting possible in CUBE_EXPLODE - cube_explode(array[ CAST(num) AS STRING, item]) AS COMBO
    // cube_explode(array[ CAST(num) AS STRING, item]) AS COMBO,
    // Received 400 response from server: line 3:30: no viable alternative at input 'array[ CAST(num)'. Error code: 40001

    val createCubeStreamSql =
      s"""CREATE STREAM $cubedStreamName AS SELECT
         | id,
         | cube_explode(array[name, item]) AS COMBO,
         | ts
         | FROM $streamName;""".stripMargin

    // Received 400 response from server: Cannot construct an array with mismatching types ([INTEGER, STRING]) from expression ARRAY[NUM, ITEM].. Error code: 40001

    client.executeStatement(createCubeStreamSql).get

    (1 to 2) foreach { i =>
      val ksqlObject = makeKsqlObjectScalar(i)
      setup.client.insertInto(streamName, ksqlObject).get()
    }

    val querySql               = s"""SELECT *
                           |FROM $cubedStreamName
                           |EMIT CHANGES;""".stripMargin
    val q: StreamedQueryResult = client.streamQuery(querySql).get()

    // expecting 8 result rows = permutations of 2 source values & null
//    Row{columnNames=[ID, COMBO, TS], columnTypes=[ColumnType{type=STRING}, ColumnType{type=ARRAY}, ColumnType{type=BIGINT}], values=["id1",[null,null],1611496969700]}
//    Row{columnNames=[ID, COMBO, TS], columnTypes=[ColumnType{type=STRING}, ColumnType{type=ARRAY}, ColumnType{type=BIGINT}], values=["id1",[null,"hMUqLo"],1611496969700]}
//    Row{columnNames=[ID, COMBO, TS], columnTypes=[ColumnType{type=STRING}, ColumnType{type=ARRAY}, ColumnType{type=BIGINT}], values=["id1",["6OvA",null],1611496969700]}
//    Row{columnNames=[ID, COMBO, TS], columnTypes=[ColumnType{type=STRING}, ColumnType{type=ARRAY}, ColumnType{type=BIGINT}], values=["id1",["6OvA","hMUqLo"],1611496969700]}
//    Row{columnNames=[ID, COMBO, TS], columnTypes=[ColumnType{type=STRING}, ColumnType{type=ARRAY}, ColumnType{type=BIGINT}], values=["id2",[null,null],1611496969728]}
//    Row{columnNames=[ID, COMBO, TS], columnTypes=[ColumnType{type=STRING}, ColumnType{type=ARRAY}, ColumnType{type=BIGINT}], values=["id2",[null,"gDKe4C"],1611496969728]}
//    Row{columnNames=[ID, COMBO, TS], columnTypes=[ColumnType{type=STRING}, ColumnType{type=ARRAY}, ColumnType{type=BIGINT}], values=["id2",["CSjW",null],1611496969728]}
//    Row{columnNames=[ID, COMBO, TS], columnTypes=[ColumnType{type=STRING}, ColumnType{type=ARRAY}, ColumnType{type=BIGINT}], values=["id2",["CSjW","gDKe4C"],1611496969728]}
    (1 to 10) foreach { _ =>
      val row: Row = q.poll(pollTimeout)
      println(row)
    }
  }

  // existing tests do not cover cube_exploding arrays
  "cube with string arrays" in {
    val topicName       = "arrayCubeSource"
    val streamName      = "arrayCubeSourceStream"
    val cubedStreamName = "arrayCubeTargetStream"

    TestHelper.prepareTest(
      streamsToDelete = List(streamName, cubedStreamName),
      topicsToCreate = List(topicName),
      client = client,
      adminClient = adminClient
    )

    val createArrayStreamSql =
      s"""CREATE STREAM $streamName(
         | id STRING KEY,
         | items ARRAY<STRING>,
         | names ARRAY<STRING>,
         | ts BIGINT)
         | WITH (
         | kafka_topic='$topicName',
         | value_format='json',
         | timestamp = 'ts'
         | );""".stripMargin
    client.executeStatement(createArrayStreamSql).get

    val createCubeStreamSql =
      s"""CREATE STREAM $cubedStreamName AS SELECT
         | id,
         | cube_explode(array[names, items]) AS COMBO,
         | ts
         | FROM $streamName;""".stripMargin

    client.executeStatement(createCubeStreamSql).get

    (1 to 2) foreach { i =>
      val ksqlObject = makeKsqlObjectTwoArrays(i)
      setup.client.insertInto(streamName, ksqlObject).get()
    }

    val querySql               = s"""SELECT *
                           |FROM $cubedStreamName
                           |EMIT CHANGES;""".stripMargin
    val q: StreamedQueryResult = client.streamQuery(querySql).get()

    // uses full array as values, does not combine values of the two arrays
    //Row{columnNames=[ID, COMBO, TS], columnTypes=[ColumnType{type=STRING}, ColumnType{type=ARRAY}, ColumnType{type=BIGINT}], values=["id1",[null,null],1611497342394]}
    //Row{columnNames=[ID, COMBO, TS], columnTypes=[ColumnType{type=STRING}, ColumnType{type=ARRAY}, ColumnType{type=BIGINT}], values=["id1",[null,["yLdS5B","rt","QW","bUIKbU"]],1611497342394]}
    //Row{columnNames=[ID, COMBO, TS], columnTypes=[ColumnType{type=STRING}, ColumnType{type=ARRAY}, ColumnType{type=BIGINT}], values=["id1",[["9","S","bXf","yTR"],null],1611497342394]}
    //Row{columnNames=[ID, COMBO, TS], columnTypes=[ColumnType{type=STRING}, ColumnType{type=ARRAY}, ColumnType{type=BIGINT}], values=["id1",[["9","S","bXf","yTR"],["yLdS5B","rt","QW","bUIKbU"]],1611497342394]}
    //Row{columnNames=[ID, COMBO, TS], columnTypes=[ColumnType{type=STRING}, ColumnType{type=ARRAY}, ColumnType{type=BIGINT}], values=["id2",[null,null],1611497342426]}
    //Row{columnNames=[ID, COMBO, TS], columnTypes=[ColumnType{type=STRING}, ColumnType{type=ARRAY}, ColumnType{type=BIGINT}], values=["id2",[null,["3mgugH","DZ","vb","NIOuHC"]],1611497342426]}
    //Row{columnNames=[ID, COMBO, TS], columnTypes=[ColumnType{type=STRING}, ColumnType{type=ARRAY}, ColumnType{type=BIGINT}], values=["id2",[["H","h","bqR","Qt5"],null],1611497342426]}
    //Row{columnNames=[ID, COMBO, TS], columnTypes=[ColumnType{type=STRING}, ColumnType{type=ARRAY}, ColumnType{type=BIGINT}], values=["id2",[["H","h","bqR","Qt5"],["3mgugH","DZ","vb","NIOuHC"]],1611497342426]}
    // expecting 8 result rows = permutations of 2 source values & null
    (1 to 10) foreach { _ =>
      val row: Row = q.poll(pollTimeout)
      println(row)
    }
  }

  def makeKsqlObjectScalar(idSuffix: Int): KsqlObject =
    new KsqlObject()
      .put("id", "id" + idSuffix.toString)
      .put("item", Random.alphanumeric.take(6).mkString)
      .put("name", Random.alphanumeric.take(4).mkString)
      .put("ts", System.currentTimeMillis())

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
      Random.alphanumeric.take(1).mkString,
      Random.alphanumeric.take(1).mkString,
      Random.alphanumeric.take(3).mkString,
      Random.alphanumeric.take(3).mkString
    )
    makeKsqlObjectSingleArray(idSuffix).put("names", new KsqlArray(list.asJava))
  }
}
