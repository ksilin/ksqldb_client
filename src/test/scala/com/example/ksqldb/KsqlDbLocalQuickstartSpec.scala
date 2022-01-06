package com.example.ksqldb

import java.util
import java.util.Properties
import java.util.concurrent.CompletableFuture
import io.confluent.ksql.api.client.{Client, ClientOptions, ExecuteStatementResult, KsqlObject, Row, SourceDescription, StreamInfo, StreamedQueryResult}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

import java.time.Duration
import scala.jdk.DurationConverters._
import scala.jdk.CollectionConverters._
import better.files._

import java.io.InputStream
import com.example.ksqldb.CsvTypes.RiderLocation
import com.example.ksqldb.util.KsqlSpecHelper
import com.example.ksqldb.util.KsqlSpecHelper.printSourceDescription
import kantan.csv._
import kantan.csv.ops._
import kantan.csv.generic._
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.scalatest.BeforeAndAfterAll

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class KsqlDbLocalQuickstartSpec
    extends AnyFreeSpec
    with Matchers
    with BeforeAndAfterAll
    with FutureConverter {

  val KSQLDB_SERVER_HOST      = "localhost"
  val KSQLDB_SERVER_HOST_PORT = 8088
  val options: ClientOptions = ClientOptions
    .create()
    .setHost(KSQLDB_SERVER_HOST)
    .setPort(KSQLDB_SERVER_HOST_PORT)
  val client: Client = Client.create(options)

  val streamName = "riderLocations"
  val tableName  = "mv_close"

  val adminClientProps      = new Properties()
  val adminBootstrapServers = "localhost:29092"
  adminClientProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, adminBootstrapServers)
  val adminClient: AdminClient = AdminClient.create(adminClientProps)

  override def beforeAll(): Unit = {
    // streams created as SELECT must be stopped before removal
    KsqlSpecHelper.deleteStream(streamName, client, adminClient)
    KsqlSpecHelper.deleteTable(tableName, client)
  }

  override def afterAll(): Unit = {
    client.close()
    super.afterAll()
  }

  "run the quickstart from https://ksqldb.io/quickstart.html" in {

    val streamsBefore: util.List[StreamInfo] = client.listStreams().get()
    streamsBefore.asScala.find(_.getName.equalsIgnoreCase(streamName)) mustBe empty

    val createRiderLocationStreamSql =
      s"""CREATE OR REPLACE STREAM $streamName
         | (profileId VARCHAR KEY, latitude DOUBLE, longitude DOUBLE, timestamp BIGINT)
         | WITH (kafka_topic='locations', value_format='json', partitions=1, timestamp='timestamp');""".stripMargin

    val createdRiderLocationStream: ExecuteStatementResult =
      client.executeStatement(createRiderLocationStreamSql).get
    createdRiderLocationStream.queryId() mustBe empty // no persistent query

    // must have our new stream now
    val streamsAfter: util.List[StreamInfo] = client.listStreams().get
    streamsAfter.asScala.find(_.getName.equalsIgnoreCase(streamName)) must not be empty

    val describeStream: SourceDescription = client.describeSource(streamName).get
    println(s"describing stream $streamName:")
    printSourceDescription(describeStream)

    val selectCloseRidersPushQuery =
      s"""SELECT * FROM $streamName
         |WHERE GEO_DISTANCE(latitude, longitude, 37.4133, -122.1162) <= 5
         |EMIT CHANGES;""".stripMargin

    val closeRiderPushQuery: StreamedQueryResult =
      client.streamQuery(selectCloseRidersPushQuery).get
    // only push queries have an Id, pull queries terminate after being finished
    println(s"createdCloseRiderPushQuery: $closeRiderPushQuery")

    val selectCloseRidersPushQueryAsTable =
      s"""CREATE TABLE $tableName AS
         |SELECT profileId, COUNT(profileId)
         |FROM $streamName
         |WHERE GEO_DISTANCE(latitude, longitude, 37.4133, -122.1162) <= 5
         |GROUP BY profileId
         |EMIT CHANGES;""".stripMargin

    val createdCloseRiderMatPushStatement: ExecuteStatementResult =
      client.executeStatement(selectCloseRidersPushQueryAsTable).get
    createdCloseRiderMatPushStatement.queryId() mustNot be(empty)
    println(s"created table: $createdCloseRiderMatPushStatement")
    val describeTable: SourceDescription = client.describeSource(tableName).get
    println(s"describing table $tableName:")
    printSourceDescription(describeTable)

    val pushQueryPollTimeout = Duration.ofMillis(100)
    val firstRow: Row        = closeRiderPushQuery.poll(pushQueryPollTimeout)
    firstRow mustBe null // no data yet

    val riderLocations: List[RiderLocation] = readRiderLocationsFromFileUnsafe("riderLocations.csv")

    Future.traverse(riderLocations) { rl: RiderLocation =>
      println(s"riderLocation: $rl")
      val row = new KsqlObject()
        .put("profileId", rl.profileId)
        .put("latitude", rl.latitude)
        .put("longitude", rl.longitude)
        .put("timestamp", rl.timestamp)
      val insert: CompletableFuture[Void] = client.insertInto(streamName, row)
      toScalaFuture(insert).map(_ => println(s"inserted $rl"))
    }

    println("polling push query results:")
    (1 until 5).foreach { _ =>
      val row: Row = closeRiderPushQuery.poll(pushQueryPollTimeout)
      println(
        s"are we done/failed yet? ${closeRiderPushQuery.isComplete}/${closeRiderPushQuery.isFailed}"
      )
      println(row)
    }

    // Pull queries require a WHERE clause that limits the query to a single key, e.g. `SELECT * FROM X WHERE myKey=Y;`
    val selectCloseRidersPullQuery = s"SELECT * FROM $tableName WHERE profileId = '4ab5cbad';"
    // now as pull query:
    val rowsFromPullQuery: mutable.Buffer[Row] =
      client.executeQuery(selectCloseRidersPullQuery).get.asScala
    rowsFromPullQuery.size mustBe 1

    // no results from a terminated query:
    client.terminatePushQuery(closeRiderPushQuery.queryID()).get
    Thread.sleep(200) // need to wait for the query to actually terminate

    // push query does not terminate
    println(
      s"are we done/failed yet? ${closeRiderPushQuery.isComplete}/${closeRiderPushQuery.isFailed}"
    )
    val row: Row = closeRiderPushQuery.poll(pushQueryPollTimeout)
    println(row)

    Thread.sleep(1000) // make sure we dont break the polling
  }

  def readRiderLocationsFromFileUnsafe(resourceFileName: String): List[RiderLocation] = {
    import CsvTypes.RiderLocation
    val ksqlLines: InputStream                       = Resource.getAsStream(resourceFileName)
    val reader: CsvReader[ReadResult[RiderLocation]] = ksqlLines.asCsvReader[RiderLocation](rfc)
    println("inserting data")
    val (locationsMaybe, failures) = reader.toList.partition(_.isRight)
    if (failures.nonEmpty) {
      println(s"found ${failures.size} failures:")
      failures foreach println
    }
    println(s"found ${locationsMaybe.size} results")
    locationsMaybe.map(_.toOption.get)
  }
}
