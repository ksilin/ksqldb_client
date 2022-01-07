package com.example.ksqldb

import com.example.ksqldb.util.TestData.{User, random}
import com.example.ksqldb.util.{CCloudClientProps, CCloudSetup, ClientProps, KsqlConnectionSetup, KsqlSpecHelper, RecordProcessor, SpecBase}
import com.fasterxml.jackson.databind.JsonNode
import io.circe.generic.auto._
import io.confluent.ksql.api.client.{Client, ExecuteStatementResult, Row, StreamedQueryResult}
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition

import java.time.Duration
import scala.jdk.CollectionConverters._
import java.util.Properties
import scala.util.Random

case class RequestMsg(id: String, userId: String, timestamp: Long)

class ApiCallSpec extends SpecBase {

  private val clientProps: ClientProps = CCloudClientProps.create(configPath = Some("cloud.stag.local"))
  private val setup: KsqlConnectionSetup =
    CCloudSetup(ksqlHost = "localhost", ksqlDbPort = 8088, clientProps)
  private val ksqlClient: Client           = setup.client
  private val adminClient: AdminClient = setup.adminClient
  private val pollTimeout: Duration    = Duration.ofMillis(1000)

  val users: Seq[User]           = random[User](50).distinctBy(_.id).take(5)
  val userIds: Seq[String]       = users.map(_.id)
  val userMap: Map[String, User] = (users map (d => d.id -> d)).toMap

  override def afterAll(): Unit = {
    ksqlClient.close()
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
    val q: StreamedQueryResult = ksqlClient.streamQuery(queryResponseStreamSql).get

    val requestConsumer = createRequestConsumer
    requestConsumer.assign(List(new TopicPartition(requestsTopicName, 0)).asJava)

    produceUserDataRequests(userIds, requestsTopicName)

    val responseProducer = JsonStringProducer[String, User](
      clientProperties = setup.commonProps,
      topic = responseTopicName,
      clientId = "responseProducer"
    )
    RecordProcessor.fetchAndProcessRecords[String, JsonNode](requestConsumer, processRecord(responseProducer))

    // fetch data from response stream
    (1 to userIds.size) foreach { _ =>
      val row: Row = q.poll(pollTimeout)
      println(row)
    }
  }

  // this is our naive processing
  def processRecord(
      responseProducer: JsonStringProducer[String, User]
  ): ConsumerRecord[String, JsonNode] => Unit = { r: ConsumerRecord[String, JsonNode] =>
    info(s"processing record: ${r.key()} | ${r.key()}")
    val userId = r.value().get("userId").asText("FALLBACK_ID")
    info(s"fetching user with id: $userId")

    fakeAPICall(userId).fold(warn(s"no user found for id $userId")) { u: User =>
      info(s"user found: $u")
      val record: ProducerRecord[String, String] = responseProducer.makeRecord(userId, u)
      responseProducer.run(record)
    }
  }

  def fakeAPICall(userID: String): Option[User] = {
    val responseDelay = Random.nextInt(1000)
    info(s"API response delay $responseDelay")
    Thread.sleep(responseDelay)
    userMap.get(userID)
  }

  def produceUserDataRequests(
      userIds: Seq[String],
      topicName: String,
      commonProps: Properties = setup.commonProps
  ): Unit = {
    val requestProducer = JsonStringProducer[String, RequestMsg](
      clientProperties = commonProps,
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
    val consumerProperties = new Properties()
    consumerProperties.putAll(setup.commonProps)
    consumerProperties.put(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer"
    )
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerProperties.put(
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.connect.json.JsonDeserializer"
    )
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "requestConsumer")

    new KafkaConsumer[String, JsonNode](consumerProperties)
  }

  def prepareTest(): ExecuteStatementResult = {

    val bootstrapServer: String = setup.commonProps.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG).toString
    val rf: Short = if(bootstrapServer.contains("cloud")) 3 else 1

    KsqlSpecHelper.prepareTest(
      streamsToDelete = List(requestsStreamName, responseStreamName),
      topicsToDelete = List(requestsTopicName, responseTopicName),
      topicsToCreate = List(requestsTopicName, responseTopicName),
      client = ksqlClient,
      adminClient = adminClient,
      replicationFactor = rf
    )
    // create request & response streams
    ksqlClient.executeStatement(createRequestStreamSql).get
    ksqlClient.executeStatement(createResponseStreamSql).get
  }

}
