package com.example.ksqldb

import java.util
import java.util.Properties

import com.example.ksqldb.TestData._
import io.circe.generic.auto._
import io.confluent.ksql.api.client.{Client, ClientOptions, ExecuteStatementResult, Row}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class KsqlDbJoinSpec extends AnyFreeSpec with Matchers with BeforeAndAfterAll { // with FutureConverter {

  val KSQLDB_SERVER_HOST = "localhost"
  val KSQLDB_SERVER_HOST_PORT = 8088

  val options: ClientOptions = ClientOptions.create()
    .setHost(KSQLDB_SERVER_HOST)
    .setPort(KSQLDB_SERVER_HOST_PORT)

  val client: Client = Client.create(options)

  val adminClientProps = new Properties()
  private val adminBootstrapServers = "localhost:29092"
  adminClientProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, adminBootstrapServers)
  val adminClient: AdminClient = AdminClient.create(adminClientProps)

  val users: Seq[User] = random[User](50).distinctBy(_.id).take(5)
  val userIds: Seq[String] = users.map(_.id)

  val clicks = random[Click](100)

  val userTopicName = "users"
  val clickTopicName = "clicks"

  val userTableName = "userTable"
  val clickStreamName = "clickStream"
  val joinedStreamName = "joinedStream"


  override def beforeAll(): Unit = {

    TestHelper.deleteTable(userTableName, client)
    TestHelper.deleteStream(clickStreamName, client, adminClient)
    TestHelper.deleteStream(joinedStreamName, client, adminClient)

    TestHelper.createTopic(adminClient, userTopicName, 1, 1)
    TestHelper.createTopic(adminClient, clickTopicName, 1, 1)

    val userProducer = JsonStringProducer[String, User](adminBootstrapServers, userTopicName)
    val userRecords = userProducer.makeRecords((users map (d => d.id -> d)).toMap)
    userProducer.run(userRecords)

    val clickProducer = JsonStringProducer[String, Click](adminBootstrapServers, clickTopicName)
    val clickRecords = clickProducer.makeRecords((clicks map (d => d.userId -> d)).toMap)
    clickProducer.run(clickRecords)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    client.close()
    super.afterAll()
  }

  "create simple user-click join" in {

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

    val createClickStreamSql = s"CREATE STREAM $clickStreamName (userId VARCHAR KEY, element VARCHAR, userAgent VARCHAR, timestamp BIGINT ) WITH (kafka_topic='clicks', value_format='json', timestamp = 'timestamp');"

    val stream: ExecuteStatementResult = client.executeStatement(createClickStreamSql).get()
    println(stream)


    val joinedStreamSql =
      s"""CREATE STREAM ${joinedStreamName} AS
         | SELECT
         | u.id as userId,
         | u.name as userName,
         | c.element,
         | c.timestamp as ts
         | FROM $clickStreamName c
         | LEFT JOIN $userTableName u ON c.userId = u.id
         | EMIT CHANGES;""".stripMargin

    val joinedStreamCreated =  client.executeStatement(joinedStreamSql).get
    println("joinedStreamCreated")
    println(joinedStreamCreated)
  }

}