package com.example.ksqldb

import java.io.PrintStream
import java.time.Duration
import java.util.Properties

import com.example.ksqldb.TestData._
import io.circe.generic.auto._
import io.confluent.ksql.api.client.{Client, ClientOptions, ExecuteStatementResult, Row, StreamedQueryResult}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import monix.execution.Ack
import monix.execution.Ack.Continue
import monix.reactive._
import monix.execution.Scheduler.{global => scheduler}

import scala.jdk.CollectionConverters._

class KsqlDbJoinSpec extends AnyFreeSpec with Matchers with BeforeAndAfterAll { // with FutureConverter {

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

  val users: Seq[User]     = random[User](50).distinctBy(_.id).take(5)
  val userIds: Seq[String] = users.map(_.id)
  val clicks: Seq[Click] = random[Click](100)
  val userTopicName  = "users"
  val clickTopicName = "clicks"
  val userTableName    = "userTable"
  val clickStreamName  = "clickStream"
  val leftJoinedStreamName = "joinedStreamLeft"
  val innerJoinedStreamName = "joinedStreamInner"


  override def beforeAll(): Unit = {

    TestHelper.deleteTable(userTableName, client)
    TestHelper.deleteStream(clickStreamName, client, adminClient)
    TestHelper.deleteStream(leftJoinedStreamName, client, adminClient)
    TestHelper.deleteStream(innerJoinedStreamName, client, adminClient)

    TestHelper.deleteTopic(clickTopicName, adminClient)
    TestHelper.deleteTopic(userTopicName, adminClient)

    TestHelper.createTopic(clickTopicName, adminClient, 1, 1)
    TestHelper.createTopic(userTopicName, adminClient, 1, 1)

    val userProducer = JsonStringProducer[String, User](adminBootstrapServers, userTopicName)
    val userRecords  = userProducer.makeRecords((users map (d => d.id -> d)).toMap)
    userProducer.run(userRecords)

    val clickProducer = JsonStringProducer[String, Click](adminBootstrapServers, clickTopicName)
    val clickRecords  = clickProducer.makeRecords((clicks map (d => d.userId -> d)).toMap)

    clickProducer.run(clickRecords)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    client.close()
    super.afterAll()
  }
  // https://docs.ksqldb.io/en/latest/developer-guide/joins/join-streams-and-tables/#stream-table-joins
  "create simple user-click stream-table join" in {

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
    // in examples, I see emit changes: https://docs.ksqldb.io/en/latest/tutorials/examples/#joining

    client.executeStatement(createUsersTableSql).get

    val createClickStreamSql =
      s"CREATE STREAM $clickStreamName (userId VARCHAR KEY, element VARCHAR, userAgent VARCHAR, timestamp BIGINT ) WITH (kafka_topic='clicks', value_format='json', timestamp = 'timestamp');"

    val stream: ExecuteStatementResult = client.executeStatement(createClickStreamSql).get()
    println(stream)

    // only LEFT and INNER are supported

    // table updates do nto produce new output, only updates on the stream side do

    // assumed that all records are processed in timestamp order

    // can contain NULL values for user name
    val leftJoinedStreamSql =
      s"""CREATE STREAM ${leftJoinedStreamName} AS
         | SELECT
         | c.userId as userId,
         | u.name as userName,
         | c.element,
         | c.timestamp as ts
         | FROM $clickStreamName c
         | LEFT JOIN $userTableName u ON c.userId = u.id
         | EMIT CHANGES;""".stripMargin

    val leftJoinedStreamCreated: ExecuteStatementResult =
      client.executeStatement(leftJoinedStreamSql).get
    println("leftJoinedStreamCreated")
    println(leftJoinedStreamCreated)

    val leftJoinedQuerySql                = s"SELECT * FROM $leftJoinedStreamName EMIT CHANGES;"
    val queryCreated: StreamedQueryResult = client.streamQuery(leftJoinedQuerySql).get
    queryCreated.subscribe(TestHelper.makeRowObserver("leftJoin").toReactive(scheduler))

    // no nulls
    val innerJoinedStreamSql =
      s"""CREATE STREAM ${innerJoinedStreamName} AS
         | SELECT
         | c.userId as userId,
         | u.name as userName,
         | c.element,
         | c.timestamp as ts
         | FROM $clickStreamName c
         | INNER JOIN $userTableName u ON c.userId = u.id
         | EMIT CHANGES;""".stripMargin

    val innerJoinedStreamCreated: ExecuteStatementResult =
      client.executeStatement(innerJoinedStreamSql).get
    println("innerJoinedStreamCreated")
    println(innerJoinedStreamCreated)

    val innerJoinedQuerySql                = s"SELECT * FROM $innerJoinedStreamName EMIT CHANGES;"
    val queryCreatedInner: StreamedQueryResult = client.streamQuery(innerJoinedQuerySql).get
    queryCreatedInner.subscribe(TestHelper.makeRowObserver("innerJoin").toReactive(scheduler))

    Thread.sleep(1000)
  }

}
