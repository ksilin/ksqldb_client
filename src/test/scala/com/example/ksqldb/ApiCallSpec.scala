package com.example.ksqldb

import com.example.ksqldb.TestData.{ User, random }
import com.fasterxml.jackson.databind.JsonNode
import io.circe.generic.auto._
import io.confluent.ksql.api.client.{ Client, ExecuteStatementResult, Row, StreamedQueryResult }
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.{
  ConsumerConfig,
  ConsumerRecord,
  ConsumerRecords,
  KafkaConsumer
}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.{ BeforeAndAfterAll, BeforeAndAfterEach }
import wvlet.log.LogSupport

import java.time
import java.time.Duration
import scala.jdk.CollectionConverters._
import java.util.Properties
import scala.util.Random

case class RequestMsg(id: String, userId: String, timestamp: Long)

class ApiCallSpec
    extends AnyFreeSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with FutureConverter
    with LogSupport {

  private val setup: LocalSetup        = LocalSetup()
  private val client: Client           = setup.client
  private val adminClient: AdminClient = setup.adminClient
  private val pollTimeout: Duration    = Duration.ofMillis(1000)

  val users: Seq[User]           = random[User](50).distinctBy(_.id).take(5)
  val userIds: Seq[String]       = users.map(_.id)
  val userMap: Map[String, User] = (users map (d => d.id -> d)).toMap

  override def afterAll(): Unit = {
    client.close()
    super.afterAll()
  }

  val requestsTopicName  = "requests"
  val requestsStreamName = "requestStream"

  val responseTopicName  = "responses"
  val responseStreamName = "responseStream"

  val errorTopicName  = "requestErrors"
  val errorStreamName = "requestErrorStream"

  val createRequestStreamSql: String =
    s"""CREATE OR REPLACE STREAM $requestsStreamName
       | (id VARCHAR KEY, userId STRING, timestamp BIGINT)
       | WITH (kafka_topic='$requestsTopicName', value_format='json', partitions=1);""".stripMargin

  val createResponseStreamSql: String =
    s"""CREATE OR REPLACE STREAM $responseStreamName(
       | id VARCHAR KEY,
       | name VARCHAR,
       | address STRUCT <
       | street VARCHAR,
       | building VARCHAR,
       | index VARCHAR >)
       |WITH (kafka_topic='$responseTopicName', value_format='json', partitions=1);""".stripMargin

  "request & response streams" in {
    prepareTest()

    // query the response stream
    val queryResponseStreamSql = s"""SELECT * FROM $responseStreamName EMIT CHANGES;"""
    val q: StreamedQueryResult = client.streamQuery(queryResponseStreamSql).get

    val requestConsumer = createRequestConsumer
    requestConsumer.assign(List(new TopicPartition(requestsTopicName, 0)).asJava)

    produceUserDataRequests(userIds, requestsTopicName)

    val responseProducer = JsonStringProducer[String, User](
      bootstrapServers = setup.adminClientBootstrapServer,
      topic = responseTopicName,
      clientId = "responseProducer"
    )
    fetchAndProcessRecords[JsonNode](requestConsumer, processRecord(responseProducer))

    // fetch data from response stream
    (1 to userIds.size) foreach { _ =>
      val row: Row = q.poll(pollTimeout)
      println(row)
    }
  }

  def fetchAndProcessRecords[T](
      consumer: KafkaConsumer[String, T],
      process: ConsumerRecord[String, T] => Any = { r: ConsumerRecord[String, T] =>
        info(r.value())
      },
      duration: time.Duration = java.time.Duration.ofMillis(100),
      maxAttempts: Int = 100
  ): Unit = {
    var done     = false
    var attempts = 0
    while (!done && attempts < maxAttempts) {
      val records: ConsumerRecords[String, T] = consumer.poll(duration)
      attempts = attempts + 1
      done = !records.isEmpty
      if (done) {
        info(s"fetched ${records.count()} records on attempt $attempts")
        records.asScala map { r: ConsumerRecord[String, T] =>
          process(r)
        }
      }
    }
    if (attempts >= maxAttempts) {
      warn(s"${maxAttempts} retries exhausted")
    }
  }

  // this is our naive processing
  def processRecord(
      responseProducer: JsonStringProducer[String, User]
  ): ConsumerRecord[String, JsonNode] => Unit = { r: ConsumerRecord[String, JsonNode] =>
    info(s"processing record: ${r.key()} | ${r.key()}")
    val userId = r.value().get("userId").asText("FALLBACK_ID")
    info(s"fetching user with id: $userId")

    callAPI(userId).fold(warn(s"no user found for id $userId")) { u: User =>
      info(s"user found: ${u}")
      val record: ProducerRecord[String, String] = responseProducer.makeRecord(userId, u)
      responseProducer.run(record)
    }
  }

  def callAPI(userID: String): Option[User] = {
    val responseDelay = Random.nextInt(1000)
    info(s"API response delay $responseDelay")
    Thread.sleep(responseDelay)
    userMap.get(userID)
  }

  def produceUserDataRequests(
      userIds: Seq[String],
      topicName: String,
      bootstrapServer: String = setup.adminClientBootstrapServer
  ): Unit = {
    val requestProducer = JsonStringProducer[String, RequestMsg](
      bootstrapServers = bootstrapServer,
      topic = topicName,
      clientId = "requestProducer"
    )
    val userDataRequests: Map[String, RequestMsg] = (userIds map (uid =>
      uid -> RequestMsg(Random.alphanumeric.take(10).mkString, uid, System.currentTimeMillis())
    )).toMap
    val r: Iterable[ProducerRecord[String, String]] = requestProducer.makeRecords(userDataRequests)
    requestProducer.run(r)
  }

  def createRequestConsumer: KafkaConsumer[String, JsonNode] = {
    val properties = new Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, setup.adminClientBootstrapServer)
    properties.put(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer"
    )
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties.put(
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.connect.json.JsonDeserializer"
    )
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "requestConsumer")
    new KafkaConsumer[String, JsonNode](properties)
  }

  def prepareTest(): ExecuteStatementResult = {
    TestHelper.prepareTest(
      streamsToDelete = List(requestsStreamName, responseStreamName),
      topicsToDelete = List(requestsTopicName, responseTopicName),
      topicsToCreate = List(requestsTopicName, responseTopicName),
      client = client,
      adminClient = adminClient
    )

    // create request & response streams
    client.executeStatement(createRequestStreamSql).get
    client.executeStatement(createResponseStreamSql).get
  }

}
