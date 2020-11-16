package com.example.ksqldb

import java.util
import java.util.Properties
import java.util.concurrent.CompletableFuture

import io.confluent.ksql.api.client.{BatchedQueryResult, Client, ClientOptions, ExecuteStatementResult, KsqlObject, QueryInfo, Row, SourceDescription, StreamInfo, StreamedQueryResult, TableInfo, TopicInfo}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import java.time.Duration

import scala.jdk.DurationConverters._
import java.time.temporal.ChronoUnit

import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._
import better.files._
import java.io.InputStream

import com.example.ksqldb.TestHelper.printSourceDescription
import kantan.csv._
import kantan.csv.ops._
import kantan.csv.generic._
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.scalatest.BeforeAndAfterAll

class KsqlDbLocalQuickstartSpec extends AnyFreeSpec with Matchers with BeforeAndAfterAll { // with FutureConverter {

  val KSQLDB_SERVER_HOST = "localhost"
  val KSQLDB_SERVER_HOST_PORT = 8088

  val options: ClientOptions = ClientOptions.create()
    .setHost(KSQLDB_SERVER_HOST)
    .setPort(KSQLDB_SERVER_HOST_PORT)

  val client: Client = Client.create(options)

  // stream can be deleted without stopping it
  // streams created as SELECT must be stopped before removal
  // remove stream if exists
  val streamName = "riderLocations"
  val tableName = "mv_close"

  val adminClientProps = new Properties()
  private val adminBootstrapServers = "localhost:29092"
  adminClientProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, adminBootstrapServers)
  val adminClient: AdminClient = AdminClient.create(adminClientProps)

  TestHelper.deleteStream(streamName, client, adminClient)
  TestHelper.deleteTable(tableName, client)

  override def afterAll(): Unit = {
    client.close()
    super.afterAll()
  }

  "run the quickstart from https://ksqldb.io/quickstart.html" in {

    val listStreamsBefore: CompletableFuture[util.List[StreamInfo]] = client.listStreams()
    val streamsBefore: util.List[StreamInfo] = listStreamsBefore.get()
    streamsBefore.asScala.find(_.getName.equalsIgnoreCase(streamName)) mustBe empty

    val createStreamStatement = s"CREATE OR REPLACE STREAM $streamName (profileId VARCHAR KEY, latitude DOUBLE, longitude DOUBLE, timestamp BIGINT) WITH (kafka_topic='locations', value_format='json', partitions=1, timestamp='timestamp');"

    val selectCloseRidersPushQuery = s"SELECT * FROM $streamName WHERE GEO_DISTANCE(latitude, longitude, 37.4133, -122.1162) <= 5 EMIT CHANGES;"

    val selectCloseRidersPushQueryAsTable = s"CREATE TABLE $tableName AS SELECT profileId, COUNT(profileId) FROM $streamName WHERE GEO_DISTANCE(latitude, longitude, 37.4133, -122.1162) <= 5 GROUP BY profileId EMIT CHANGES;"

    // Pull queries require a WHERE clause that limits the query to a single key, e.g. `SELECT * FROM X WHERE myKey=Y;`
    val selectCloseRidersPullQuery = s"SELECT * FROM $tableName WHERE profileId = '4ab5cbad';"

    val createStream: CompletableFuture[ExecuteStatementResult] = client.executeStatement(createStreamStatement)
    val createdStream: ExecuteStatementResult = createStream.get()
    println(s"created stream: $createdStream")

    val describeStream: SourceDescription = client.describeSource(streamName).get
    println(s"describing stream $streamName:")
    printSourceDescription(describeStream)

    // must have our new stream now
    val listStreamsAfter: CompletableFuture[util.List[StreamInfo]] = client.listStreams()
    val streamsAfter: util.List[StreamInfo] = listStreamsAfter.get()
    streamsAfter.asScala.find(_.getName.equalsIgnoreCase(streamName)) must not be (empty)

    val createCloseRiderPushQuery: CompletableFuture[StreamedQueryResult] = client.streamQuery(selectCloseRidersPushQuery)
    val createdCloseRiderPushQuery: StreamedQueryResult = createCloseRiderPushQuery.get()

    // only push queries have an Id, pull queries terminate after being finished
    println(s"createdCloseRiderPushQuery: $createdCloseRiderPushQuery")

    // TODO - can I explain queries with the client? - not with the describe
    // val describePushQuery: SourceDescription = client.describeSource(???).get
    // println(s"explaining query: $describePushQuery")

    val createCloseRiderMatPushStatement: CompletableFuture[ExecuteStatementResult] = client.executeStatement(selectCloseRidersPushQueryAsTable)
    val createdCloseRiderMatPushStatement: ExecuteStatementResult = createCloseRiderMatPushStatement.get()
    println(s"created table: $createdCloseRiderMatPushStatement")

    val describeTable: SourceDescription = client.describeSource(tableName).get
    println(s"describing table $tableName:")
    printSourceDescription(describeTable)
    // TODO - I have an 'emit changes' in the create table statement, how do I get the results from an ExecuteStatementResult?

    // val createCloseRiderMatPushQuery: CompletableFuture[StreamedQueryResult] = client.streamQuery(selectCloseRidersPushQueryAsTable)
    // val createdCloseRiderMatPushQuery: StreamedQueryResult = createCloseRiderMatPushQuery.get()
    //println(s"createdCloseRiderMatPushQuery: $createdCloseRiderMatPushQuery")

    val pollTimeout = Duration.ofMillis(100)
    // should be empty - no data
    val firstRow: Row = createdCloseRiderPushQuery.poll(pollTimeout)
    firstRow mustBe null

    import CsvTypes.RiderLocation
    val ksqlLines: InputStream = Resource.getAsStream("riderLocations.csv")

    val reader: CsvReader[ReadResult[RiderLocation]] = ksqlLines.asCsvReader[RiderLocation](rfc)//.withCellSeparator(';'))
    println("inserting data")
    val (locationsMaybe, failures) = reader.toList.partition(_.isRight)

    println(s"found ${failures.size} failures:")
    failures foreach println

    println(s"found ${locationsMaybe.size} results")

    locationsMaybe foreach { riderLocation: ReadResult[RiderLocation] =>
      println(s"riderLocation: $riderLocation")
      riderLocation.map { rl =>
        val row = new KsqlObject().put("profileId", rl.profileId).put("latitude", rl.latitude).put("longitude", rl.longitude).put("timestamp", rl.timestamp)
        val insert: CompletableFuture[Void] = client.insertInto(streamName, row)
        insert.get
      }
    }

    println("polling results:")
    (1 until 5).foreach { _ =>
      val row: Row = createdCloseRiderPushQuery.poll(pollTimeout)
      println(s"are we done/failed yet? ${createdCloseRiderPushQuery.isComplete}/${createdCloseRiderPushQuery.isFailed}")
      println(row)
    }

    // now as pull query:
    val createPullQuery: BatchedQueryResult = client.executeQuery(selectCloseRidersPullQuery)
    val createdPullQuery: util.List[Row] = createPullQuery.get()
    println("pull query:")
    createdPullQuery.asScala foreach println

    Thread.sleep(1000) // make sure we dont break the polling
  }

  "produce some test data" in {

    val topicName = "producerTestTopic"

    TestHelper.createTopic(adminClient, topicName, 1, 1)

    import TestData._
    import io.circe.generic.auto._

    val sensorReadings: Seq[SensorData] =  random[SensorData](10)

    val producer = JsonStringProducer[String, SensorData](adminBootstrapServers, topicName)

    val records = producer.makeRecords((sensorReadings map (d => d.id -> d)).toMap)
    producer.run(records)
  }



}