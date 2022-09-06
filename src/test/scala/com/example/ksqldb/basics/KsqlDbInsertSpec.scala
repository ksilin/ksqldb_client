package com.example.ksqldb.basics

import com.example.ksqldb.util._
import io.confluent.ksql.api.client.exception.KsqlClientException
import io.confluent.ksql.api.client.{ KsqlObject, Row, StreamedQueryResult }
import monix.execution.Scheduler.{ global => scheduler }
import monix.reactive.Observable
import org.reactivestreams.Publisher
import org.scalatest.BeforeAndAfterEach

import java.util.concurrent.CompletableFuture
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.jdk.FutureConverters._
import scala.jdk.CollectionConverters._

class KsqlDbInsertSpec
    extends SpecBase(configPath = Some("ccloud.ps.ksilin.dedicated_ksilin"))
    with BeforeAndAfterEach {

  val streamName = "toBeInsertedInto"

  val createStreamSql: String =
    s"""CREATE OR REPLACE STREAM $streamName
       | (id VARCHAR KEY, amount INT)
       | WITH (kafka_topic='KsqlDbInsertSpec', value_format='json', partitions=1);""".stripMargin

  val pushQuerySql: String =
    s"""SELECT * FROM $streamName
       |EMIT CHANGES;""".stripMargin

  "must fail on inserting data missing the key column" in {

    val row = new KsqlObject()
      .put("marco", "polo")
      .put("uga", "aga")

    val insert: Future[Void] = ksqlClient
      .executeStatement(createStreamSql)
      .asScala
      .flatMap(_ => ksqlClient.insertInto(streamName, row).asScala)
    // io.confluent.ksql.api.client.exception.KsqlClientException: Received error from /inserts-stream. Error code: 40000. Message: Key field must be specified: ID
    val failure = intercept[KsqlClientException](Await.result(insert, 60.seconds))
    println(failure.getMessage)
    failure.getMessage.contains("Key field must be specified: ID") mustBe true
  }

  "must succeed on inserting data not conforming to schema, as long as ID field is contained" in {
    val insertData = new KsqlObject()
      .put("ID", "someId")
      .put("foo", 1)
      .put("bar", 1.23f)

    val insert: Future[Void] = ksqlClient
      .executeStatement(createStreamSql)
      .asScala
      .flatMap(_ => ksqlClient.insertInto(streamName, insertData).asScala)
    // io.confluent.ksql.api.client.exception.KsqlClientException: Received error from /inserts-stream. Error code: 40000. Message: Key field must be specified: ID
    Await.result(insert, 60.seconds)

    // now get the data back:
    val pushQuery: StreamedQueryResult =
      ksqlClient.streamQuery(pushQuerySql).get

    // reading the data will only yield known columns
    val row: Row = pushQuery.poll()
    row.columnNames().asScala must contain theSameElementsAs List("ID", "AMOUNT")
    row.getString("ID") mustBe "someId"
    row.getInteger("AMOUNT") mustBe null
  }

  "must insert rows into stream via Client.insertInto" in {

    val preparePushQuery: Future[StreamedQueryResult] =
      ksqlClient
        .executeStatement(createStreamSql)
        .asScala
        .flatMap(_ => ksqlClient.streamQuery(pushQuerySql).asScala)
    val pushQuery: StreamedQueryResult = Await.result(preparePushQuery, 60.seconds)

    val ids = (1 to 10).toList
    val insertAll: Future[List[Unit]] = Future.traverse(ids) { i: Int =>
      val row = new KsqlObject()
        .put("id", i.toString)
        .put("amount", i)
      val insert: CompletableFuture[Void] = ksqlClient.insertInto(streamName, row)
      insert.asScala.map(_ => info(s"inserted $i"))
    }
    Await.result(insertAll, 60.seconds)

    val row: Row = pushQuery.poll()
    row.columnNames().asScala must contain theSameElementsAs List("ID", "AMOUNT")
    row.getString("ID") mustBe "1"
    row.getInteger("AMOUNT") mustBe 1
  }

  // https://github.com/confluentinc/ksql/pull/5641/files
  "must insert rows into stream via reactive publisher" in {

    val preparePushQuery: Future[StreamedQueryResult] =
      ksqlClient
        .executeStatement(createStreamSql)
        .asScala
        .flatMap(_ => ksqlClient.streamQuery(pushQuerySql).asScala)
    val pushQuery: StreamedQueryResult = Await.result(preparePushQuery, 60.seconds)

    val observable = Observable.range(10, 0, -1).map { i =>
      new KsqlObject()
        .put("id", i.toString)
        .put("amount", i)
    }
    val pub: Publisher[KsqlObject] = observable.toReactivePublisher(scheduler)
    val insertAll                  = ksqlClient.streamInserts(streamName, pub).asScala
    Await.result(insertAll, 60.seconds)

    val row: Row = pushQuery.poll()
    row.columnNames().asScala must contain theSameElementsAs List("ID", "AMOUNT")
    row.getString("ID") mustBe "10"
    row.getInteger("AMOUNT") mustBe 10
  }

  // TODO - show failure on inserting NULL / Tombstones

  override def beforeEach(): Unit =
    KsqlSpecHelper.deleteStream(streamName, ksqlClient, setup.adminClient)

  override def afterAll(): Unit = {
    ksqlClient.close()
    super.afterAll()
  }

}
