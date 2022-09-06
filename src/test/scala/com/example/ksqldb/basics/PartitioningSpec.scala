package com.example.ksqldb.basics

import com.example.ksqldb.util._
import io.confluent.ksql.api.client.{KsqlObject, Row, StreamedQueryResult}
import org.scalatest.BeforeAndAfterEach

import java.util.concurrent.CompletableFuture
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.jdk.FutureConverters._
import scala.jdk.DurationConverters._

class PartitioningSpec
    extends SpecBase(configPath = Some("ccloud.ps.ksilin.dedicated_ksilin"))
    with BeforeAndAfterEach {

  val prefix: String = suiteName
  val topicName = s"${prefix}_topic"
  val streamName = s"${prefix}_stream"

  val createStreamSql: String =
    s"""CREATE OR REPLACE STREAM $streamName
       | (id VARCHAR KEY, amount INT)
       | WITH (kafka_topic='$topicName', value_format='json', partitions=3);""".stripMargin

  val pushQuerySql: String =
    s"""SELECT ROWPARTITION AS P,
       | COUNT(ROWPARTITION) AS PC
       | FROM $streamName
       | WINDOW TUMBLING ( SIZE 10 SECONDS, GRACE PERIOD 1 SECOND)
       | GROUP BY ROWPARTITION
       | EMIT CHANGES;""".stripMargin

  // HISTOGRAM does nto work on integers for some reason, need to cast here
  val pushQueryHistogramSql: String =
    s"""SELECT ROWPARTITION AS P,
       | HISTOGRAM(CAST(ROWPARTITION AS STRING)) AS PC
       | FROM $streamName
       | WINDOW TUMBLING ( SIZE 10 SECONDS, GRACE PERIOD 1 SECOND)
       | GROUP BY ROWPARTITION
       | EMIT CHANGES;""".stripMargin

  "aggregating partition metadata" - {

    KsqlSpecHelper.deleteStream(streamName, ksqlClient, setup.adminClient)
    ksqlClient
      .executeStatement(createStreamSql, queryProperties).get()

    val ids = (1 to 10).toList
    val insertAll: Future[List[Unit]] = Future.traverse(ids) { i: Int =>
      val row = new KsqlObject()
        .put("id", i.toString)
        .put("amount", i)
      val insert: CompletableFuture[Void] = ksqlClient.insertInto(streamName, row)
      insert.asScala.map(_ => info(s"inserted $i"))
    }
    Await.result(insertAll, 60.seconds)

    "COUNT" in {

      val query: StreamedQueryResult = ksqlClient.streamQuery(pushQuerySql, queryProperties).get()

      println("querying the results: ")
      var row: Row = query.poll(10000.millis.toJava)
      var rows: List[Row] = Nil
      while (row != null) {
        info(row.values())
        rows = row :: rows
        row = query.poll(10000.millis.toJava)
      }
      println(s"found ${rows.size} result rows")
      val partitionCounts: List[(Integer, Integer)] = rows.map { r =>
              (r.getInteger("P"), r.getInteger("PC"))
      }
      partitionCounts foreach println
    }

    "HISTOGRAM" in {

      val pushQuery: StreamedQueryResult = ksqlClient.streamQuery(pushQueryHistogramSql, queryProperties).get

      println("querying the results: ")
      var row: Row = pushQuery.poll(10000.millis.toJava)
      var rows: List[Row] = Nil
      while (row != null) {
        info(row.values())
        rows = row :: rows
        row = pushQuery.poll(10000.millis.toJava)
      }
      println(s"found ${rows.size} result rows")
      val partitionCounts: List[(Integer, KsqlObject)] = rows.map { r =>
        (r.getInteger("P"), r.getKsqlObject("PC"))
      }
      partitionCounts foreach println
    }


  }

  // override def beforeEach(): Unit =
   // KsqlSpecHelper.deleteStream(streamName, ksqlClient, setup.adminClient)

  override def afterAll(): Unit = {
    ksqlClient.close()
    super.afterAll()
  }

}
