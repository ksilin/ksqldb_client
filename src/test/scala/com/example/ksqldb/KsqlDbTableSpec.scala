package com.example.ksqldb

import com.example.ksqldb.util.KsqlSpecHelper

import java.util
import java.util.Properties
import java.util.concurrent.ExecutionException
import com.example.ksqldb.util.TestData._
import io.circe.generic.auto._
import io.confluent.ksql.api.client.exception.KsqlClientException
import io.confluent.ksql.api.client.{Client, ClientOptions, ExecuteStatementResult, Row}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class KsqlDbTableSpec extends AnyFreeSpec with Matchers with BeforeAndAfterAll { // with FutureConverter {

  val KSQLDB_SERVER_HOST      = "localhost"
  val KSQLDB_SERVER_HOST_PORT = 8088

  val options: ClientOptions = ClientOptions
    .create()
    .setHost(KSQLDB_SERVER_HOST)
    .setPort(KSQLDB_SERVER_HOST_PORT)

  val client: Client = Client.create(options)

  val adminClientProps              = new Properties()
  private val adminBootstrapServers = "localhost:29092"
  adminClientProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, adminBootstrapServers)
  val adminClient: AdminClient = AdminClient.create(adminClientProps)

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

  override def beforeAll(): Unit = {

//    TestHelper.deleteStream(userStreamName, client, adminClient)
//    TestHelper.deleteTable(userTableName, client)
//
//    TestHelper.deleteStream(productStreamName, client, adminClient)
//    TestHelper.deleteTable(productTableName, client)
//    TestHelper.deleteTable(productTableName2, client)
//
//    TestHelper.deleteStream(orderStreamName, client, adminClient)

    KsqlSpecHelper.createTopic(userTopicName, adminClient, 1, 1)
    KsqlSpecHelper.createTopic(productTopicName, adminClient, 1, 1)
    KsqlSpecHelper.createTopic(orderTopicName, adminClient, 1, 1)

    val userProducer = JsonStringProducer[String, User](adminClientProps, userTopicName)
    val userRecords  = userProducer.makeRecords((users map (d => d.id -> d)).toMap)
    userProducer.run(userRecords)

    val productProducer =
      JsonStringProducer[String, Product](adminClientProps, productTopicName)
    val productRecords = productProducer.makeRecords((products map (d => d.id -> d)).toMap)
    productProducer.run(productRecords)

//    val orderProducer = JsonStringProducer[String, Order](adminBootstrapServers, orderTopicName)
//    val orderRecords = orderProducer.makeRecords((orders map (d => d.id -> d)).toMap)
//    orderProducer.run(orderRecords)

    super.beforeAll()
  }

  override def afterAll(): Unit = {
    client.close()
    super.afterAll()
  }

  "create test products" in {

    val productProducer2 =
      JsonStringProducer[String, Product](adminClientProps, productTopicName)
    val productRecords2 = productProducer2.makeRecords((products map (d => d.id -> d)).toMap)
    productProducer2.run(productRecords2)
  }

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

    client.executeStatement(createUsersTableSql).get

    val pullQueryFromTable = s"SELECT * FROM $userTableName WHERE id = '1';"

    // Can't pull from `USERTABLE` as it's not a materialized table
    val t: Throwable = intercept[Throwable](client.executeQuery(pullQueryFromTable).get)
    t mustBe a[ExecutionException]
    t.getCause mustBe a[KsqlClientException]
    t.getMessage.contains("not a materialized table") mustBe true

    val limit: Int              = 3
    val pushQueryFromTable      = s"SELECT * FROM $userTableName EMIT CHANGES LIMIT $limit;"
    val resPush: util.List[Row] = client.executeQuery(pushQueryFromTable).get
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

    val t: Throwable = intercept[Throwable](client.executeStatement(createProductTableSql).get)
    t mustBe a[ExecutionException]
    t.getCause mustBe a[KsqlClientException]
    t.getMessage.contains("mismatched input 'EMIT' expecting ';'") mustBe true
  }

  "create table from stream - can emit changes?" in {

    val createUserStreamSql =
      s"CREATE STREAM $productStreamName (id VARCHAR KEY, description VARCHAR) WITH (kafka_topic='products', value_format='json');"

    val stream: ExecuteStatementResult = client.executeStatement(createUserStreamSql).get()
    println(stream)

    // CTAS
    // Your SELECT query produces a STREAM. Please use CREATE STREAM AS SELECT statement instead
    val createProductTableSql_stream = s"""CREATE TABLE $productTableName AS SELECT
                                    |id , description
                                    |FROM $productStreamName
                                    |EMIT CHANGES;
                                    |""".stripMargin

    // requires an AGG
    val err = intercept[Throwable](client.executeStatement(createProductTableSql_stream).get)
    err.getMessage.contains(
      "our SELECT query produces a STREAM. Please use CREATE STREAM AS SELECT statement instead"
    ) mustBe true

    // GROUP BY requires aggregate functions in either the SELECT or HAVING clause
    val createProductTableSql_noAgg = s"""CREATE TABLE $productTableName AS SELECT
                                    |id , description
                                    |FROM $productStreamName
                                    |GROUP BY id
                                    |EMIT CHANGES;
                                    |""".stripMargin

    val errNoAgg = intercept[Throwable](client.executeStatement(createProductTableSql_noAgg).get)
    errNoAgg.getMessage.contains(
      "GROUP BY requires aggregate functions in either the SELECT or HAVING clause"
    ) mustBe true

    // Use of aggregate function COUNT requires a GROUP BY clause
    val createProductTableSql_noGroupBy = s"""CREATE TABLE $productTableName AS SELECT
         |id , description, count(description)
         |FROM $productStreamName
         |EMIT CHANGES;
         |""".stripMargin

    val errNoGroupBy = intercept[Throwable] {
      client.executeStatement(createProductTableSql_noGroupBy).get
    }
    errNoGroupBy.getMessage.contains(
      "Use of aggregate function COUNT requires a GROUP BY clause"
    ) mustBe true

    // Non-aggregate SELECT expression(s) not part of GROUP BY: DESCRIPTION
    val createProductTableSql_nonAggNotInGroupBy = s"""CREATE TABLE $productTableName AS SELECT
                                    |id , description, count(description)
                                    |FROM $productStreamName
                                    |GROUP BY id
                                    |EMIT CHANGES;
                                    |""".stripMargin

    val errNonAggNotInGroupBy = intercept[Throwable] {
      client.executeStatement(createProductTableSql_nonAggNotInGroupBy).get
    }
    errNonAggNotInGroupBy.getMessage.contains(
      "Non-aggregate SELECT expression(s) not part of GROUP BY"
    ) mustBe true

    // works, contains no data
    // if executed from CLI, terminates without data
    val createProductTableSql_simpleGroupBy = s"""CREATE TABLE $productTableName AS SELECT
                                    |id, count_distinct(description) as ct
                                    |FROM $productStreamName
                                    |GROUP BY id
                                    |EMIT CHANGES;
                                    |""".stripMargin

    val createT: ExecuteStatementResult =
      client.executeStatement(createProductTableSql_simpleGroupBy).get
    println(createT)

    // grouping by all non-aggregates works, but the compund key is strange:
//    ksql> print 'ksql_tst_topic-PRODUCTTABLE' from beginning;
//    Key format: HOPPING(KAFKA_STRING) or TUMBLING(KAFKA_STRING) or KAFKA_STRING
//    Value format: JSON or KAFKA_STRING
//    rowtime: 2020/11/20 04:33:28.532 Z, key: [hyperfreeze|+@8954411046315125103/-], value: {"KSQL_COL_0":1}
//    rowtime: 2020/11/20 04:33:28.634 Z, key: [promogalore|+|Ka@7955434415124406638/-], value: {"KSQL_COL_0":1}
//    rowtime: 2020/11/20 04:33:28.737 Z, key: [bonanzi@8947381722297757299/-], value: {"KSQL_COL_0":1}
//    rowtime: 2020/11/20 04:33:28.839 Z, key: [fashionergy@8947381812659904884/-], value: {"KSQL_COL_0":1}
//    rowtime: 2020/11/20 04:33:28.941 Z, key: [bargainizer|+@8956922313491573605/-], value: {"KSQL_COL_0":1}
//    rowtime: 2020/11/20 04:33:29.043 Z, key: [strikelane|+|@5575851516132160112/-], value: {"KSQL_COL_0":1}
//    rowtime: 2020/11/20 04:33:29.146 Z, key: [mysteryessence|@3133466733342978920/-], value: {"KSQL_COL_0":1}

    val createProductTableSql_compGroupBy = s"""CREATE TABLE "$productTableName2" AS SELECT
                                    |id , description, count(description) AS ct
                                    |FROM $productStreamName
                                    |GROUP BY id, description
                                    |EMIT CHANGES;
                                    |""".stripMargin

    // TODO - dropping the table fails, bc of missing source, need to clarify
    //  val productTableCreated = client.executeStatement(createProductTableSql_compGroupBy).get
    //  println(productTableCreated)

    // pull queries possible, but need to wait:
    // KeyQueryMetadata not available for state store Aggregate-Aggregate-Materialize and key Struct{X=X}
    // https://github.com/confluentinc/ksql/issues/6249
    // or
    // Unable to execute pull query: SELECT * FROM productTable WHERE id = 'bargainizer';

    Thread.sleep(500)
    val pullQueryFromTable       = s"SELECT * FROM $productTableName WHERE id = '${prodIds.head}';"
    val res: mutable.Buffer[Row] = client.executeQuery(pullQueryFromTable).get.asScala
    println("result:")
    res foreach println
    res.size mustBe 1
    res.head.getInteger("CT") mustBe 1
    res.head.values().getList.asScala foreach println

//    val pullQueryFromTable2 = s"SELECT * FROM $productTableName2 WHERE id = '${prodIds.head}';"
//    val res2: util.List[Row] = client.executeQuery(pullQueryFromTable2).get
//    println("result2:")
//    res2.asScala foreach println
  }

}
