package com.example.ksqldb.basics

import com.example.ksqldb.JsonStringProducer
import com.example.ksqldb.util.TestData._
import com.example.ksqldb.util._
import io.circe.generic.auto._
import io.confluent.ksql.api.client.Row
import io.confluent.ksql.api.client.exception.KsqlClientException

import java.util
import java.util.concurrent.ExecutionException
import scala.collection.mutable
import scala.jdk.CollectionConverters._

class KsqlDbTableSpec extends SpecBase(configPath = Some("ccloud.stag.local")) {

  val users: Seq[User]        = random[User](50).distinctBy(_.id).take(5)
  val userIds: Seq[String]    = users.map(_.id)
  val products: Seq[Product]  = random[Product](50).distinctBy(_.id).take(prodIds.size)
  val productIds: Seq[String] = products.map(_.id)
  val orders: Seq[Order] =
    random[Order](100).filter(o => userIds.contains(o.userId) && productIds.contains(o.prodId))

  val userTopicName    = "users"
  val productTopicName = "products"
  val orderTopicName   = "orders"

  val userStreamName    = "userStream"
  val userTableName     = "userTable"
  val productStreamName = "productStream"
  val productTableName  = "productTable"
  val productTableName2 = "productTable2"
  val orderStreamName   = "orderStream"

  "table with existing topic - not materialized -> no pull queries, but push queries" in {

    val createUsersTableSql =
      s"""CREATE TABLE $userTableName (
         | id VARCHAR PRIMARY KEY,
         | name VARCHAR,
         | address STRUCT <
         | street VARCHAR,
         | building VARCHAR,
         | index VARCHAR
         | >
         | ) WITH (
         | KAFKA_TOPIC = '$userTopicName',
         | VALUE_FORMAT = 'JSON'
         | );""".stripMargin

    ksqlClient.executeStatement(createUsersTableSql).get

    val pullQueryFromTable = s"SELECT * FROM $userTableName WHERE id = '1';"

    // Can't pull from `USERTABLE` as it's not a materialized table
    val t: Throwable = intercept[Throwable](ksqlClient.executeQuery(pullQueryFromTable).get)
    t mustBe a[ExecutionException]
    t.getCause mustBe a[KsqlClientException]
    println(t.getCause)
    t.getMessage.contains("table isn't queryable") mustBe true
    //t.getMessage.contains("not a materialized table") mustBe true - for earlier versions

    val limit: Int              = 3
    val pushQueryFromTable      = s"SELECT * FROM $userTableName EMIT CHANGES LIMIT $limit;"
    val resPush: util.List[Row] = ksqlClient.executeQuery(pushQueryFromTable).get
    resPush.size() mustBe limit
  }

  // TODO - is EMIT CHANGES a syntax error here?
  "create table with existing topic - cannot EMIT CHANGES" in {

    val createProductTableSql =
      s"""CREATE TABLE $productTableName (
        |id VARCHAR PRIMARY KEY,
        | description VARCHAR
        |) WITH (
        |KAFKA_TOPIC = '$orderTopicName',
        |VALUE_FORMAT = 'JSON'
        |) EMIT CHANGES;
        |""".stripMargin

    val t: Throwable = intercept[Throwable](ksqlClient.executeStatement(createProductTableSql).get)
    t mustBe a[ExecutionException]
    t.getCause mustBe a[KsqlClientException]
    t.getMessage.contains("mismatched input 'EMIT' expecting ';'") mustBe true
  }

  "creating and querying tables" - {

    "create table from stream with 'emit changes' must fail - create a stream instead" in {

      // CTAS
      // Your SELECT query produces a STREAM. Please use CREATE STREAM AS SELECT statement instead
      val createProductTableSql_stream = s"""CREATE TABLE $productTableName AS SELECT
                                    |id , description
                                    |FROM $productStreamName
                                    |EMIT CHANGES;
                                    |""".stripMargin

      // requires an AGG
      val err = intercept[Throwable](ksqlClient.executeStatement(createProductTableSql_stream).get)
      println(err)
      err.getMessage.contains(
        "our SELECT query produces a STREAM. Please use CREATE STREAM AS SELECT statement instead"
      ) mustBe true

    }

    "create table with 'group by' without aggregate fn must fail" in {

      // GROUP BY requires aggregate functions in either the SELECT or HAVING clause
      val createProductTableSql_noAgg = s"""CREATE TABLE $productTableName AS SELECT
                                    |id , description
                                    |FROM $productStreamName
                                    |GROUP BY id
                                    |EMIT CHANGES;
                                    |""".stripMargin

      val errNoAgg =
        intercept[Throwable](ksqlClient.executeStatement(createProductTableSql_noAgg).get)
      errNoAgg.getMessage.contains(
        "GROUP BY requires aggregate functions in either the SELECT or HAVING clause"
      ) mustBe true
    }

    "create table with an aggregate fn but without a 'group by' must fail" in {

      // Use of aggregate function COUNT requires a GROUP BY clause
      val createProductTableSql_noGroupBy = s"""CREATE TABLE $productTableName AS SELECT
         |id , description, count(description)
         |FROM $productStreamName
         |EMIT CHANGES;
         |""".stripMargin

      val errNoGroupBy = intercept[Throwable] {
        ksqlClient.executeStatement(createProductTableSql_noGroupBy).get
      }
      println(errNoGroupBy.getMessage)
      errNoGroupBy.getMessage.contains(
        "Use of aggregate function COUNT requires a GROUP BY clause"
      ) mustBe true

    }

    "fails if not all non-aggregate SELECT expressions are part of GROUP BY" in {

      // Non-aggregate SELECT expression(s) not part of GROUP BY: DESCRIPTION
      val createProductTableSql_nonAggNotInGroupBy = s"""CREATE TABLE $productTableName AS SELECT
                                    |id , description, count(description)
                                    |FROM $productStreamName
                                    |GROUP BY id
                                    |EMIT CHANGES;
                                    |""".stripMargin

      val errNonAggNotInGroupBy = intercept[Throwable] {
        ksqlClient.executeStatement(createProductTableSql_nonAggNotInGroupBy).get
      }
      errNonAggNotInGroupBy.getMessage.contains(
        "Non-aggregate SELECT expression(s) not part of GROUP BY"
      ) mustBe true
    }

    "works if all non-aggregate SELECT expressions are part of GROUP BY - by fails if KEY_FORMAT does not support multiple columns" in {

      // works, contains no data
      // if executed from CLI, terminates without data
      val createProductTableSql_simpleGroupBy =
        s"""CREATE TABLE $productTableName WITH (KEY_FORMAT = 'DELIMITED') AS SELECT
                                    |id, description, count_distinct(description) as ct
                                    |FROM $productStreamName
                                    |GROUP BY id, description
                                    |EMIT CHANGES;
                                    |""".stripMargin

      ksqlClient.executeStatement(createProductTableSql_simpleGroupBy).get

      // PULL queries dont support LIMIT clauses ... LIMIT 1;
      Thread.sleep(10000) // newly created table needs some time to populate
      val pullQueryFromTable       = s"SELECT * FROM $productTableName WHERE id = '${productIds.head}';"
      val res: mutable.Buffer[Row] = ksqlClient.executeQuery(pullQueryFromTable).get.asScala
      println("result:")
      res foreach println
//    res.size mustBe 1
//    res.head.getInteger("CT") mustBe 1
//    res.head.values().getList.asScala foreach println
    }
  }

  override def beforeAll(): Unit = {

    KsqlSpecHelper.deleteTable(userTableName, ksqlClient)
    KsqlSpecHelper.deleteStream(userStreamName, ksqlClient, adminClient)

    KsqlSpecHelper.deleteTable(productTableName, ksqlClient)
    KsqlSpecHelper.deleteTable(productTableName2, ksqlClient)
    KsqlSpecHelper.deleteStream(productStreamName, ksqlClient, adminClient)

    KsqlSpecHelper.deleteStream(orderStreamName, ksqlClient, adminClient)

    KsqlSpecHelper.createTopic(userTopicName, adminClient, replicationFactor)
    KsqlSpecHelper.createTopic(productTopicName, adminClient, replicationFactor)
    KsqlSpecHelper.createTopic(orderTopicName, adminClient, replicationFactor)

    val userProducer = JsonStringProducer[String, User](setup.commonProps, userTopicName)
    val userRecords  = userProducer.makeRecords((users map (d => d.id -> d)).toMap)
    userProducer.run(userRecords)

    val productProducer =
      JsonStringProducer[String, Product](setup.commonProps, productTopicName)
    val productRecords = productProducer.makeRecords((products map (d => d.id -> d)).toMap)
    productProducer.run(productRecords)

    val createUserStreamSql =
      s"CREATE OR REPLACE STREAM $productStreamName (id VARCHAR KEY, description VARCHAR) WITH (kafka_topic='products', value_format='json');"
    ksqlClient.executeStatement(createUserStreamSql).get()

    super.beforeAll()
  }

  override def afterAll(): Unit = {
    ksqlClient.close()
    super.afterAll()
  }

}
