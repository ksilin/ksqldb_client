package com.example.ksqldb.basics

import com.example.ksqldb.util._
import io.confluent.ksql.api.client.{ ExecuteStatementResult, KsqlObject, StreamedQueryResult }
import monix.execution.Scheduler.{ global => scheduler }
import monix.reactive.Observable
import org.reactivestreams.Publisher
import org.scalatest.BeforeAndAfterEach

import java.util.concurrent.CompletableFuture
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class KsqlDbInsertSpec
    extends SpecBase(configPath = Some("ccloud.stag.local"))
    with BeforeAndAfterEach {

  val streamName = "toBeInsertedInto"

  val createStreamSql: String =
    s"""CREATE OR REPLACE STREAM $streamName
       | (id VARCHAR KEY, amount INT)
       | WITH (kafka_topic='KsqlDbInsertSpec', value_format='json', partitions=1);""".stripMargin

  val selectUnder6PushQuery: String =
    s"""SELECT * FROM $streamName
       |WHERE AMOUNT <= 5
       |EMIT CHANGES;""".stripMargin

  "must fail on inserting data breaking the stream schema"

  "must succeed on inserting 'wider' data - all fields in schema contained, but also some additional ones"

  "must insert rows into stream via Client.insertInto" in {

    val createdStream: ExecuteStatementResult =
      ksqlClient.executeStatement(createStreamSql).get
    val pushQuery: StreamedQueryResult =
      ksqlClient.streamQuery(selectUnder6PushQuery).get

    pushQuery.subscribe(KsqlSpecHelper.makeRowObserver("insert test: ").toReactive(scheduler))

    val ids = (1 to 10).toList
    Future.traverse(ids) { i: Int =>
      val row = new KsqlObject()
        .put("id", i.toString)
        .put("amount", i)
      val insert: CompletableFuture[Void] = ksqlClient.insertInto(streamName, row)
      toScalaFuture(insert).map(_ => info(s"inserted $i"))
    }
    Thread.sleep(200) // make sure we don't break the polling
    // no results from a terminated query:
    ksqlClient.terminatePushQuery(pushQuery.queryID()).get
    Thread.sleep(200) // need to wait for the query to actually terminate
    // push query does not terminate
    info(
      s"is the push query done/failed yet? ${pushQuery.isComplete}/${pushQuery.isFailed}"
    )
  }

  // https://github.com/confluentinc/ksql/pull/5641/files
  "must insert rows into stream via reactive publisher" in {

    val createdStream: ExecuteStatementResult =
      ksqlClient.executeStatement(createStreamSql).get
    val pushQuery: StreamedQueryResult =
      ksqlClient.streamQuery(selectUnder6PushQuery).get

    pushQuery.subscribe(KsqlSpecHelper.makeRowObserver("insert test: ").toReactive(scheduler))

    val observable = Observable.range(10, 0, -1).map { i =>
      new KsqlObject()
        .put("id", i.toString)
        .put("amount", i)
    }
    val pub: Publisher[KsqlObject] = observable.toReactivePublisher(scheduler)

    ksqlClient.streamInserts(streamName, pub)

    Thread.sleep(200) // make sure we dont break the polling
    // no results from a terminated query:
    ksqlClient.terminatePushQuery(pushQuery.queryID()).get
    Thread.sleep(200) // need to wait for the query to actually terminate
    // push query does not terminate
    info(
      s"is push query done/failed yet? ${pushQuery.isComplete}/${pushQuery.isFailed}"
    )
  }

  override def beforeEach(): Unit =
    KsqlSpecHelper.deleteStream(streamName, ksqlClient, setup.adminClient)

  override def afterAll(): Unit = {
    ksqlClient.close()
    super.afterAll()
  }

}
