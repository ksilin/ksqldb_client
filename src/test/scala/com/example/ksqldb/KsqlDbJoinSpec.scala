package com.example.ksqldb

import java.io.PrintStream
import java.time.{Duration, Instant}
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
import org.apache.kafka.common.serialization.Serdes.UUID
import org.scalacheck.Arbitrary
import wvlet.log.LogSupport

import scala.jdk.CollectionConverters._

class KsqlDbJoinSpec extends AnyFreeSpec with Matchers with BeforeAndAfterAll with FutureConverter with LogSupport {

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

  val users: Seq[User]      = random[User](50).distinctBy(_.id).take(5)
  val userIds: Seq[String]  = users.map(_.id)
  val clicks: Seq[Click]    = random[Click](100)
  val clicksFromKnownUsers: Seq[Click]    = clicks.filter(c => userIds.contains(c.userId))
  val userTopicName         = "users"
  val clickTopicName        = "clicks"
  val userStreamName        = "userStream"
  val userTableName         = "userTable"
  val clickStreamName       = "clickStream"
  val leftJoinedStreamName  = "joinedStreamLeft"
  val innerJoinedStreamName = "joinedStreamInner"
  val userClickstreamStreamToStreamJoinName = "userClickstreamStreamToStreamJoin"

  val now: Long = System.currentTimeMillis()
  val step = 10000
  val orderTimestamps: Seq[Long] = (0 to 50).map(now - step * _).reverse
  val orders: Seq[Order] = random[Order](50).filter(o => userIds.contains(o.userId))
  info(s"created ${orders.size} orders")

  val ordersWithNewTimestamps: Seq[Order] = orders.zip(orderTimestamps).map{ case(o, ts) => o.copy(timestamp = ts) }

  val orderIds: Seq[String] = orders.map(_.id)
  implicit val shipmentGen: Arbitrary[Shipment] = makeGenShipment(orderIds)
  val shipments: Map[String, Shipment] = random[Shipment](100).map(o => (o.orderId, o)).toMap // .distinctBy(_.orderId)

  // using collect as partial fn from orderId to shipment -> ordered shipments
  val orderedShipments: Seq[Shipment] = orderIds.collect(shipments)

  // shipments have a delay after orders
  val shipmentsWithNewTimestamps = orderedShipments.zip(orderTimestamps.takeRight(orderedShipments.size)).map{case (s, ts) => s.copy(timestamp = ts)}

  info("timestamps:")
  info(Instant.ofEpochMilli(ordersWithNewTimestamps.head.timestamp))
  info(Instant.ofEpochMilli(shipmentsWithNewTimestamps.head.timestamp))

  val orderTopicName = "orders"
  val shipmentTopicName = "shipments"

  val orderStreamName = "orderStream"
  val shipmentStreamName = "shipmentStream"
  val orderShipmentJoinedStreamName = "orderShipmentStream"


  def prepareClicksAndUsers() = {
    TestHelper.deleteTable(userTableName, client)
    TestHelper.deleteStream(clickStreamName, client, adminClient)
    TestHelper.deleteStream(leftJoinedStreamName, client, adminClient)
    TestHelper.deleteStream(innerJoinedStreamName, client, adminClient)
    TestHelper.deleteStream(userStreamName, client, adminClient)
    TestHelper.deleteStream(userClickstreamStreamToStreamJoinName, client, adminClient)

    TestHelper.deleteTopic(clickTopicName, adminClient)
    TestHelper.deleteTopic(userTopicName, adminClient)

    TestHelper.createTopic(clickTopicName, adminClient, 1, 1)
    TestHelper.createTopic(userTopicName, adminClient, 1, 1)

    val userProducer = JsonStringProducer[String, User](adminBootstrapServers, userTopicName)
    val userRecords  = userProducer.makeRecords((users map (d => d.id -> d)).toMap)
    userProducer.run(userRecords)

    val clickProducer = JsonStringProducer[String, Click](adminBootstrapServers, clickTopicName)
    val clickRecords  = clickProducer.makeRecords((clicksFromKnownUsers map (d => d.userId -> d)))

    clickProducer.run(clickRecords)

    val createClickStreamSql =
      s"CREATE STREAM $clickStreamName (userId VARCHAR KEY, element VARCHAR, userAgent VARCHAR, timestamp BIGINT ) WITH (kafka_topic='$clickTopicName', value_format='json', timestamp = 'timestamp');"

    val clickStreamCreated: ExecuteStatementResult = client.executeStatement(createClickStreamSql).get()
    info(clickStreamCreated)
  }

  def prepareOrdersAndShipments() = {

    TestHelper.deleteStream(orderStreamName, client, adminClient)
    TestHelper.deleteStream(shipmentStreamName, client, adminClient)
    TestHelper.deleteStream(orderShipmentJoinedStreamName, client, adminClient)

    TestHelper.deleteTopic(orderTopicName, adminClient)
    TestHelper.deleteTopic(shipmentTopicName, adminClient)

    TestHelper.createTopic(orderTopicName, adminClient, 1, 1)
    TestHelper.createTopic(shipmentTopicName, adminClient, 1, 1)

    // val orderProducer = JsonStringProducer[String, Order](adminBootstrapServers, orderTopicName)
    // val orderRecords  = orderProducer.makeRecords((ordersWithNewTimestamps map (d => d.id -> d)).toMap)
    // orderProducer.run(orderRecords)

    // val shipmentProducer = JsonStringProducer[String, Shipment](adminBootstrapServers, shipmentTopicName)
    // val shipmentRecords  = shipmentProducer.makeRecords((shipmentsWithNewTimestamps map (d => d.id -> d)).toMap)
    // shipmentProducer.run(shipmentRecords)

    val createOrderStreamSql =
      s"CREATE STREAM $orderStreamName (id VARCHAR KEY, userId VARCHAR, prodId VARCHAR, amount INT, location VARCHAR, timestamp BIGINT ) WITH (kafka_topic='$orderTopicName', value_format='json', timestamp = 'timestamp');"

    val orderStreamCreated: ExecuteStatementResult = client.executeStatement(createOrderStreamSql).get()
    info(orderStreamCreated)

    val createShipmentStreamSql =
      s"CREATE STREAM $shipmentStreamName (id VARCHAR KEY, orderId VARCHAR, warehouse VARCHAR, timestamp BIGINT ) WITH (kafka_topic='$shipmentTopicName', value_format='json', timestamp = 'timestamp');"

    val shipmentStreamCreated: ExecuteStatementResult = client.executeStatement(createShipmentStreamSql).get()
    info(shipmentStreamCreated)
  }


  override def afterAll(): Unit = {
    client.close()
    super.afterAll()
  }
  // https://docs.ksqldb.io/en/latest/developer-guide/joins/join-streams-and-tables/#stream-table-joins
  "create simple user-click stream-table join" in {

    prepareClicksAndUsers()

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

    // only LEFT and INNER are supported
    // table updates do not produce new output, only updates on the stream side do

    // can contain NULL values for user name
    val leftJoinedStreamSql =
      s"""CREATE STREAM ${leftJoinedStreamName} AS
         | SELECT
         | c.userId as userId,
         | CASE
         | WHEN u.name IS NULL THEN 'USER NAME FOR ' + userId + ' NOT_FOUND'
         | ELSE u.name
         | END as userName,
         | c.element,
         | c.timestamp as ts
         | FROM $clickStreamName c
         | LEFT JOIN $userTableName u ON c.userId = u.id
         | EMIT CHANGES;""".stripMargin

    val leftJoinedStreamCreated: ExecuteStatementResult =
      client.executeStatement(leftJoinedStreamSql).get
    info("leftJoinedStreamCreated")
    info(leftJoinedStreamCreated)

    val leftJoinedQuerySql                = s"SELECT * FROM $leftJoinedStreamName EMIT CHANGES;"
    val queryCreated: StreamedQueryResult = client.streamQuery(leftJoinedQuerySql).get
    queryCreated.subscribe(TestHelper.makeRowObserver("leftJoin").toReactive(scheduler))

    // inner join -> no nulls
    val innerJoinedStreamSql =
      s"""CREATE STREAM ${innerJoinedStreamName} AS
         | SELECT
         | c.userId as userId,
         | u.name as userName,
         | c.element,
         | c.timestamp as ts,
         | c.ROWTIME as rt
         | FROM $clickStreamName c
         | INNER JOIN $userTableName u ON c.userId = u.id
         | EMIT CHANGES;""".stripMargin

    // cannot use MAX here - requires a GROUP BY for no reason
    // max(c.ROWTIME, u.ROWTIME) as rt

    val innerJoinedStreamCreated: ExecuteStatementResult =
      client.executeStatement(innerJoinedStreamSql).get
    info("innerJoinedStreamCreated")
    info(innerJoinedStreamCreated)

    val innerJoinedQuerySql                    = s"SELECT * FROM $innerJoinedStreamName EMIT CHANGES;"
    val queryCreatedInner: StreamedQueryResult = client.streamQuery(innerJoinedQuerySql).get
    queryCreatedInner.subscribe(TestHelper.makeRowObserver("innerJoin").toReactive(scheduler))

    Thread.sleep(1000)
  }

  "stream to stream join" in {

    prepareClicksAndUsers()

    val createUserStreamSql =
      s"""CREATE STREAM $userStreamName
         | (id VARCHAR KEY,
         | name VARCHAR,
         | address STRUCT <
         | street VARCHAR,
         | building VARCHAR,
         | index VARCHAR
         | >
         | ) WITH (
         | KAFKA_TOPIC = '$userTopicName',
         | VALUE_FORMAT = 'JSON'
         ); """.stripMargin

    val userStreamCreated: ExecuteStatementResult =
      client.executeStatement(createUserStreamSql).get
    info("userStreamCreated")
    info(userStreamCreated)

    // stream-stream joins require a WITHIN clause
    val userClickstreamJoinedStreamSql =
      s"""CREATE STREAM ${userClickstreamStreamToStreamJoinName} AS
         | SELECT
         | c.userId as USERID,
         | u.name as USERNAME,
         | c.element,
         | c.timestamp as ts,
         | c.ROWTIME as rt
         | FROM $clickStreamName c
         | LEFT JOIN $userStreamName u
         | WITHIN 7 DAYS
         | ON c.userId = u.id
         | EMIT CHANGES;""".stripMargin

    val userClickstreamJoinedStreamSqlCreated: ExecuteStatementResult =
      client.executeStatement(userClickstreamJoinedStreamSql).get
    info("userClickstreamJoinedStreamSqlCreated")
    info(userClickstreamJoinedStreamSqlCreated)

    val joinedStreamQuerySql                    = s"SELECT * FROM $userClickstreamStreamToStreamJoinName EMIT CHANGES;"
    val queryCreatedInner: StreamedQueryResult = client.streamQuery(joinedStreamQuerySql).get

    // there are no unknown users
    val alertOnNullName: Row => Unit = (row: Row) => {
      val userName: String = row.getValue("USERNAME").asInstanceOf[String]
      if(null == userName) {
        info(s"found NULL userName: $row")
      }
    }

    queryCreatedInner.subscribe(TestHelper.makeRowObserver(prefix = "streamStreamJoin", nextPlugin = Some(alertOnNullName)).toReactive(scheduler))

    Thread.sleep(1000)
  }


  "join streams with repeating keys" in {


  }

  "join streams with different partition counts" in {


  }

  "windowed join - too short" in {

    prepareOrdersAndShipments()

    val tooShortWindow = "3 MINUTES"

    val orderShipmentJoinStreamSql = s"""CREATE STREAM ${orderShipmentJoinedStreamName} AS SELECT
                                           | o.userId AS USERID,
                                           | o.id AS ORDERID,
                                           | o.amount,
                                           | o.location,
                                           | o.timestamp as ORDER_TS,
                                           | s.timestamp as SHIPMENT_TS,
                                           | (s.timestamp - o.timestamp) as DELAY,
                                           | s.id AS SHIPMENTID,
                                           | s.warehouse
                                           | FROM $orderStreamName o
                                           | INNER JOIN $shipmentStreamName s
                                           | WITHIN $tooShortWindow
                                           | ON s.orderId = o.id
                                           | EMIT CHANGES;""".stripMargin

    val joinedStreamCreated: ExecuteStatementResult =
      client.executeStatement(orderShipmentJoinStreamSql).get

    val query = s"SELECT * FROM $orderShipmentJoinedStreamName EMIT CHANGES;"
    val joinedStreamQuery: StreamedQueryResult = client.streamQuery(query).get
    info(joinedStreamCreated)

    val detectMissingShipments: Row => Unit = (row: Row) => {
      val shipmentId: String = row.getString("SHIPMENTID")
      if(null == shipmentId) {
        warn(s"shipment is missing for: $row")
      }
    }
      // all orders have no shipments
    joinedStreamQuery.subscribe(TestHelper.makeRowObserver(prefix = "orderShipmentJoin").toReactive(scheduler))
    Thread.sleep(1000)
  }

  "windowed join - sufficient" in {

    prepareOrdersAndShipments()

    val sufficientWindow = "5 MINUTES"

    val orderShipmentJoinStreamSql = s"""CREATE STREAM ${orderShipmentJoinedStreamName} AS SELECT
                                                   | o.userId AS USERID,
                                                   | o.id AS ORDERID,
                                                   | o.amount,
                                                   | o.location,
                                                   | o.timestamp as ORDER_TS,
                                                   | s.timestamp as SHIPMENT_TS,
                                                   | (s.timestamp - o.timestamp) as DELAY,
                                                   | s.id AS SHIPMENTID,
                                                   | s.warehouse
                                                   | FROM $orderStreamName o
                                                   | LEFT JOIN $shipmentStreamName s
                                                   | WITHIN $sufficientWindow
                                                   | ON s.orderId = o.id
                                                   | EMIT CHANGES;""".stripMargin


    val joinedStreamCreated: ExecuteStatementResult =
      client.executeStatement(orderShipmentJoinStreamSql).get

    val query = s"SELECT * FROM $orderShipmentJoinedStreamName EMIT CHANGES;"
    val joinedStreamQuery: StreamedQueryResult = client.streamQuery(query).get
    info(joinedStreamCreated)

    joinedStreamQuery.subscribe(TestHelper.makeRowObserver(prefix = "orderShipmentJoin").toReactive(scheduler))
    Thread.sleep(1000)
  }


  "windowed join - interactive" in {

    prepareOrdersAndShipments()

    val sufficientWindow = "5 MINUTES"

    val orderShipmentJoinStreamSql = s"""CREATE STREAM ${orderShipmentJoinedStreamName} AS SELECT
                                        | o.userId AS USERID,
                                        | o.id AS ORDERID,
                                        | o.amount,
                                        | o.location,
                                        | o.timestamp as ORDER_TS,
                                        | s.timestamp as SHIPMENT_TS,
                                        | (s.timestamp - o.timestamp) as DELAY,
                                        | s.id AS SHIPMENTID,
                                        | s.warehouse
                                        | FROM $orderStreamName o
                                        | LEFT JOIN $shipmentStreamName s
                                        | WITHIN $sufficientWindow
                                        | ON s.orderId = o.id
                                        | EMIT CHANGES;""".stripMargin


    val joinedStreamCreated: ExecuteStatementResult =
      client.executeStatement(orderShipmentJoinStreamSql).get

    val query = s"SELECT * FROM $orderShipmentJoinedStreamName EMIT CHANGES;"
    val joinedStreamQuery: StreamedQueryResult = client.streamQuery(query).get
    info(joinedStreamCreated)

    joinedStreamQuery.subscribe(TestHelper.makeRowObserver(prefix = "orderShipmentJoin").toReactive(scheduler))

    val orderProducer = JsonStringProducer[String, Order](adminBootstrapServers, orderTopicName)

    val shipmentProducer = JsonStringProducer[String, Shipment](adminBootstrapServers, shipmentTopicName)

    import io.circe._
    import io.circe.syntax._

    val now: Long = System.currentTimeMillis()

    val order1 = ordersWithNewTimestamps.head.copy(timestamp = now)
    val order2 = order1.copy(id = "noShipment", timestamp = now)
    val shipment1  = shipmentsWithNewTimestamps.find(shipment => shipment.orderId == order1.id).get.copy(id = "s1", timestamp = now)// - Duration.ofMinutes(5).toMillis)
    val shipment2  = shipment1.copy(id = "s2",  timestamp = now + Duration.ofMinutes(5).toMillis)
    val shipment3  = shipment1.copy(id = "s3",  timestamp = now - Duration.ofMinutes(5).toMillis)
    val shipment4  = shipment1.copy(id = "s4",  timestamp = now + Duration.ofMinutes(5).toMillis)

    orderProducer.run(orderProducer.makeRecords(List(order1.id -> order1)))

    shipmentProducer.run(shipmentProducer.makeRecords(List(shipment1.id -> shipment1)))

    Thread.sleep(1000)

    orderProducer.run(orderProducer.makeRecords(List(order2.id -> order2)))

    shipmentProducer.run(shipmentProducer.makeRecords(List(shipment2.id -> shipment2)))
    shipmentProducer.run(shipmentProducer.makeRecords(List(shipment3.id -> shipment3)))
    shipmentProducer.run(shipmentProducer.makeRecords(List(shipment4.id -> shipment4)))

    // one window per key per event

    // 1607595536230 - 1607595296230 = 4 minutes

    Thread.sleep(10000)
  }


}
