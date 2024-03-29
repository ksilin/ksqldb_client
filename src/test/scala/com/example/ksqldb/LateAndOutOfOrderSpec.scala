package com.example.ksqldb

import com.example.ksqldb.util.{ KsqlSpecHelper, SpecBase }
import io.confluent.ksql.api.client.{ KsqlObject, StreamedQueryResult }

import java.time.Instant
import monix.execution.Scheduler.{ global => scheduler }
import monix.reactive.Observable
import org.reactivestreams.Publisher
import org.scalatest.BeforeAndAfterEach

import java.time.temporal.ChronoUnit

class LateAndOutOfOrderSpec
    extends SpecBase(configPath = Some("ccloud.ps.ksilin.basic_test"))
    with BeforeAndAfterEach {

  val pollingTimeout = 5000

  override def afterAll(): Unit = {
    ksqlClient.close()
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    KsqlSpecHelper.prepareTest(
      streamsToDelete = List(sourceStreamName),
      tablesToDelete = List(tableName),
      topicsToCreate = List(sourceTopicName),
      client = ksqlClient,
      adminClient = adminClient,
      replicationFactor = replicationFactor
    )
    ksqlClient.executeStatement(sourceStreamSql).get()
  }

  val sourceStreamName = "sourceStream"
  val sourceTopicName  = "source"
  val tableName        = "sums"

  val sourceStreamSql: String =
    s"""CREATE STREAM $sourceStreamName(id INT, v INT, ts BIGINT)
       |WITH (kafka_topic='$sourceTopicName', value_format='json', timestamp='ts');""".stripMargin

  "event time pushes stream time outside of grace period" in {

    ksqlClient
      .executeStatement(tableSql("WINDOW TUMBLING (SIZE 1 MINUTE, GRACE PERIOD 1 MINUTE)"))
      .get()

    val tableQuerySql          = s"SELECT * FROM $tableName EMIT CHANGES;"
    val q: StreamedQueryResult = ksqlClient.streamQuery(tableQuerySql).get
    q.subscribe(KsqlSpecHelper.makeRowObserver("windowed").toReactive(scheduler))

    val now             = Instant.now().truncatedTo(ChronoUnit.MINUTES)
    val startEvent      = makeKsqlObject(1, 10, now, 0)
    val outOfOrderEvent = makeKsqlObject(1, 20, now, 130)
    val lateEvent       = makeKsqlObject(1, 40, now, 10)

    val obs: Observable[KsqlObject] = Observable(startEvent, outOfOrderEvent, lateEvent)
    val pub: Publisher[KsqlObject]  = obs.toReactivePublisher(scheduler)
    ksqlClient.streamInserts(sourceStreamName, pub)

    Thread.sleep(pollingTimeout) // make sure we dont break the polling
    ksqlClient.terminatePushQuery(q.queryID()).get
    Thread.sleep(200) // need to wait for the query to actually terminate
    info(s"done: ${q.isComplete}, failed: ${q.isFailed}")
  }

  "event time pushes stream time, but remains within grace period" in {

    ksqlClient
      .executeStatement(tableSql("WINDOW TUMBLING (SIZE 1 MINUTE, GRACE PERIOD 1 MINUTE)"))
      .get()

    val tableQuerySql          = s"SELECT * FROM $tableName EMIT CHANGES;"
    val q: StreamedQueryResult = ksqlClient.streamQuery(tableQuerySql).get
    q.subscribe(KsqlSpecHelper.makeRowObserver("windowed").toReactive(scheduler))

    val now             = Instant.now().truncatedTo(ChronoUnit.MINUTES)
    val startEvent      = makeKsqlObject(1, 10, now, 0)
    val outOfOrderEvent = makeKsqlObject(1, 20, now, 110)
    val lateEvent       = makeKsqlObject(1, 40, now, 10)

    val obs: Observable[KsqlObject] = Observable(startEvent, outOfOrderEvent, lateEvent)
    val pub: Publisher[KsqlObject]  = obs.toReactivePublisher(scheduler)
    ksqlClient.streamInserts(sourceStreamName, pub)

    Thread.sleep(pollingTimeout) // make sure we dont break the polling
    ksqlClient.terminatePushQuery(q.queryID()).get
    Thread.sleep(200) // need to wait for the query to actually terminate
    info(s"done: ${q.isComplete}, failed: ${q.isFailed}")
  }

  def makeKsqlObject(id: Int, value: Int, now: Instant, delaySeconds: Int = 0): KsqlObject =
    new KsqlObject()
      .put("id", id)
      .put("v", value)
      .put("ts", now.plusSeconds(delaySeconds).toEpochMilli)

  def tableSql(windowDef: String): String =
    s"""CREATE TABLE $tableName AS
         |SELECT id, SUM(v) AS s, COUNT(v) AS c,
         |TIMESTAMPTOSTRING(WINDOWSTART, 'HH:mm:ss.SSS'),
         |TIMESTAMPTOSTRING(WINDOWEND, 'HH:mm:ss.SSS'),
         |TIMESTAMPTOSTRING(EARLIEST_BY_OFFSET(ts), 'HH:mm:ss.SSS'),
         |TIMESTAMPTOSTRING(LATEST_BY_OFFSET(ts), 'HH:mm:ss.SSS')
         |FROM $sourceStreamName
         |$windowDef
         |GROUP by id;""".stripMargin
}
