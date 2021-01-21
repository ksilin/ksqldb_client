package com.example.ksqldb

import io.confluent.ksql.api.client.{ ExecuteStatementResult, KsqlObject, StreamedQueryResult }
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach }
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import monix.execution.Scheduler.{ global => scheduler }
import monix.reactive.Observable
import org.reactivestreams.Publisher
import wvlet.log.LogSupport

import java.util.concurrent.CompletableFuture
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class KsqlDbInsertSpec
    extends AnyFreeSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with FutureConverter
    with LogSupport {

  val setup: LocalSetup = LocalSetup()
  val streamName = "toBeInsertedInto"

  override def beforeEach(): Unit =
    TestHelper.deleteStream(streamName, setup.client, setup.adminClient)

  override def afterAll(): Unit = {
    setup.client.close()
    super.afterAll()
  }

  val createStreamSql: String =
    s"""CREATE OR REPLACE STREAM $streamName
       | (id VARCHAR KEY, amount INT)
       | WITH (kafka_topic='KsqlDbInsertSpec', value_format='json', partitions=1);""".stripMargin

  val selectUnder6PushQuery: String =
    s"""SELECT * FROM $streamName
       |WHERE AMOUNT <= 5
       |EMIT CHANGES;""".stripMargin

  "must fail on inserting data breaking the stream schema"

  "must succees on inserting 'wider' data - all fields in schema contained, but also some additional ones"

  "must insert rows into stream via Client.insert" in {

    val createdStream: ExecuteStatementResult =
      setup.client.executeStatement(createStreamSql).get
    val pushQuery: StreamedQueryResult =
      setup.client.streamQuery(selectUnder6PushQuery).get

    pushQuery.subscribe(TestHelper.makeRowObserver("insert test: ").toReactive(scheduler))

    val ids = (1 to 10).toList
    Future.traverse(ids) { i: Int =>
      val row = new KsqlObject()
        .put("id", i.toString)
        .put("amount", i)
      val insert: CompletableFuture[Void] = setup.client.insertInto(streamName, row)
      toScalaFuture(insert).map(_ => info(s"inserted $i"))
    }
    Thread.sleep(200) // make sure we dont break the polling
    // no results from a terminated query:
    setup.client.terminatePushQuery(pushQuery.queryID()).get
    Thread.sleep(200) // need to wait for the query to actually terminate
    // push query does not terminate
    info(
      s"are we done/failed yet? ${pushQuery.isComplete}/${pushQuery.isFailed}"
    )
  }

  // https://github.com/confluentinc/ksql/pull/5641/files
  "must insert rows into stream via reactive publisher" in {

    val createdStream: ExecuteStatementResult =
      setup.client.executeStatement(createStreamSql).get
    val pushQuery: StreamedQueryResult =
      setup.client.streamQuery(selectUnder6PushQuery).get

    pushQuery.subscribe(TestHelper.makeRowObserver("insert test: ").toReactive(scheduler))

    val observable = Observable.range(10, 0, -1).map { i =>
      new KsqlObject()
        .put("id", i.toString)
        .put("amount", i)
    }
    val pub: Publisher[KsqlObject] = observable.toReactivePublisher(scheduler)

    setup.client.streamInserts(streamName, pub)

    Thread.sleep(200) // make sure we dont break the polling
    // no results from a terminated query:
    setup.client.terminatePushQuery(pushQuery.queryID()).get
    Thread.sleep(200) // need to wait for the query to actually terminate
    // push query does not terminate
    info(
      s"are we done/failed yet? ${pushQuery.isComplete}/${pushQuery.isFailed}"
    )
  }

}
