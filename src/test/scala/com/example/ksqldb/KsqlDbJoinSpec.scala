package com.example.ksqldb

import java.time.Duration
import java.util.Properties
import com.example.ksqldb.util.TestData._
import com.example.ksqldb.util.{EnvVarUtil, KsqlSpecHelper, LocalSetup}
import io.circe.generic.auto._
import io.confluent.ksql.api.client.{Client, ExecuteStatementResult, Row, StreamedQueryResult}
import org.apache.kafka.clients.admin.AdminClient
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers
import monix.execution.Scheduler.{global => scheduler}
import org.scalacheck.Arbitrary
import wvlet.log.{LogLevel, LogSupport, Logger}

import scala.jdk.CollectionConverters._
import scala.util.Random

class KsqlDbJoinSpec
    extends AnyFreeSpec
    with Matchers
    with BeforeAndAfterAll
    with FutureConverter
    with LogSupport {

  EnvVarUtil.setEnv("RANDOM_DATA_GENERATOR_SEED", "9153932137467828920")

  Logger.setDefaultLogLevel(LogLevel.INFO)
  val loglevelProps = new Properties()
  loglevelProps.setProperty("org.apache.kafka", LogLevel.WARN.name)
  Logger.setLogLevels(loglevelProps)

  val setup: LocalSetup        = LocalSetup()
  val client: Client           = setup.client
  val adminClient: AdminClient = setup.adminClient

  val users: Seq[User]     = random[User](50).distinctBy(_.id).take(5)
  val userIds: Seq[String] = users.map(_.id)
  val clicks: Seq[Click]   = random[Click](100)
  val (clicksFromKnownUsers: Seq[Click], clicksFromUnknownUsers: Seq[Click]) =
    clicks.partition(c => userIds.contains(c.userId))
  val userTopicName                    = "users"
  val clickTopicName                   = "clicks"
  val userStreamName                   = "userStream"
  val userTableName                    = "userTable"
  val clickStreamName                  = "clickStream"
  val leftJoinedStreamName             = "joinedStreamLeft"
  val innerJoinedStreamName            = "joinedStreamInner"
  val userClicksStreamToStreamJoinName = "userClickstreamStreamToStreamJoin"
  val orderTopicName                   = "orders"
  val shipmentTopicName                = "shipments"
  val orderStreamName                  = "orderStream"
  val shipmentStreamName               = "shipmentStream"
  val orderShipmentJoinedStreamName    = "orderShipmentStream"

  val now: Long                  = System.currentTimeMillis()
  val step                       = 10000
  val orderTimestamps: Seq[Long] = (0 to 50).map(now - step * _).reverse

  val orders: Seq[Order] = random[Order](50).filter(o => userIds.contains(o.userId))
  info(s"created ${orders.size} orders")
  val ordersWithNewTimestamps: Seq[Order] =
    orders.zip(orderTimestamps).map { case (o, ts) => o.copy(timestamp = ts) }

  val orderIds: Seq[String]                     = orders.map(_.id)
  implicit val shipmentGen: Arbitrary[Shipment] = makeGenShipment(orderIds)
  val shipments: Map[String, Shipment] =
    random[Shipment](100).map(o => (o.orderId, o)).toMap // .distinctBy(_.orderId)

  // using collect as partial fn from orderId to shipment -> ordered shipments
  val orderedShipments: Seq[Shipment] = orderIds.collect(shipments)

  // shipments have a delay after orders
  val shipmentsWithNewTimestamps: Seq[Shipment] =
    orderedShipments.zip(orderTimestamps.takeRight(orderedShipments.size)).map { case (s, ts) =>
      s.copy(timestamp = ts)
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
   ^
         | ) WITH (
         | KAFKA_TOPIC = '$userTopicName',
         | VALUE_FORMAT = 'JSON'
         | );""".stripMargin
    // in examples, I see emit changes: https://docs.ksqldb.io/en/latest/tutorials/examples/#joining

    client.executeStatement(createUsersTableSql).get

    // only LEFT and INNER are supported for stream-table
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

    val leftJoinedQuerySql                = s"SELECT * FROM $leftJoinedStreamName EMIT CHANGES;"
    val queryCreated: StreamedQueryResult = client.streamQuery(leftJoinedQuerySql).get
    queryCreated.subscribe(KsqlSpecHelper.makeRowObserver("leftJoin").toReactive(scheduler))

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
    queryCreatedInner.subscribe(KsqlSpecHelper.makeRowObserver("innerJoin").toReactive(scheduler))

    Thread.sleep(1000)
  }

  "stream to table join - interactive" in {

    prepareClicksAndUsers(produceTestData = false)

    val createUsersTableSql =
      s"""CREATE TABLE $userTableName (
         | id VARCHAR PRIMARY KEY,
         | name VARCHAR,
         | address STRUCT <
         | street VARCHAR,
         | building VARCHAR,
         | index VARCHAR
         | >,
         | changedAt BIGINT
         | ) WITH (
         | KAFKA_TOPIC = '$userTopicName',
         | VALUE_FORMAT = 'JSON',
         | timestamp = 'changedAt'
         | );""".stripMargin

    client.executeStatement(createUsersTableSql).get

    val joinedStreamSql =
      s"""CREATE STREAM ${leftJoinedStreamName} AS
         | SELECT
         | c.userId as userId,
         | CASE
         | WHEN u.name IS NULL THEN 'USER NAME FOR ' + userId + ' NOT_FOUND'
         | ELSE u.name
         | END as userName,
         | c.element,
         | c.timestamp as ts,
         | u.changedAt as userTimestamp
         | FROM $clickStreamName c
         | LEFT JOIN $userTableName u ON c.userId = u.id
         | EMIT CHANGES;""".stripMargin

    client.executeStatement(joinedStreamSql).get

    val joinedQuerySql                    = s"SELECT * FROM $leftJoinedStreamName EMIT CHANGES;"
    val queryCreated: StreamedQueryResult = client.streamQuery(joinedQuerySql).get
    queryCreated.subscribe(KsqlSpecHelper.makeRowObserver("leftJoin").toReactive(scheduler))

    // produce test data
    val userProducer = JsonStringProducer[String, User](
      setup.commonProps,
      userTopicName,
      "userProducer" + Random.alphanumeric.take(10).mkString
    )
    val clickProducer = JsonStringProducer[String, Click](
      setup.commonProps,
      clickTopicName,
      "clickProducer" + Random.alphanumeric.take(10).mkString
    )

    val now = System.currentTimeMillis()

    val firstClick =
      clicksFromKnownUsers.head.copy(userId = "1", timestamp = now - Duration.ofMinutes(1).toMillis)
    val secondClick =
      firstClick.copy(userId = "1", element = "newElement", timestamp = now)
    val thirdClick =
      firstClick.copy(userId = "2", timestamp = now - Duration.ofMinutes(1).toMillis)
    val firstClickRecord  = clickProducer.makeRecords(Map(firstClick.userId -> firstClick))
    val secondClickRecord = clickProducer.makeRecords(Map(secondClick.userId -> secondClick))
    val thirdClickRecord  = clickProducer.makeRecords(Map(thirdClick.userId -> thirdClick))

    val userForFirstClick = users.head.copy(
      id = "1",
      name = "Marcelo",
      changedAt = now
    ) // - Duration.ofMinutes(2).toMillis )
    val userForSecondClick = users.head.copy(
      id = "1",
      name = "Adam",
      changedAt = now
    ) // - Duration.ofMinutes(2).toMillis )
    val userForThirdClick = users.head.copy(
      id = "2",
      name = "Konstantin",
      changedAt = now
    ) // - Duration.ofMinutes(2).toMillis )

    val userRecordForFirstClick =
      userProducer.makeRecords(Map(userForFirstClick.id -> userForFirstClick))
    val userRecordForSecondClick =
      userProducer.makeRecords(Map(userForSecondClick.id -> userForSecondClick))
    val userRecordForThirdClick =
      userProducer.makeRecords(Map(userForThirdClick.id -> userForThirdClick))

    info("produce first click")
    clickProducer.run(firstClickRecord)
    clickProducer.run(thirdClickRecord)

    info("produce user for first click")
    userProducer.run(userRecordForFirstClick)

    Thread.sleep(1500)
    info("produce user for third click")
    userProducer.run(userRecordForThirdClick)

    info("produce user for second click")
    userProducer.run(userRecordForSecondClick)

    info("produce second click")
    clickProducer.run(secondClickRecord)

    Thread.sleep(3000)
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
      s"""CREATE STREAM ${userClicksStreamToStreamJoinName} AS
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

    val joinedStreamQuerySql                   = s"SELECT * FROM $userClicksStreamToStreamJoinName EMIT CHANGES;"
    val queryCreatedInner: StreamedQueryResult = client.streamQuery(joinedStreamQuerySql).get

    // there are no unknown users
    val alertOnNullName: Row => Unit = (row: Row) => {
      val userName: String = row.getValue("USERNAME").asInstanceOf[String]
      if (null == userName) {
        info(s"found NULL userName: $row")
      }
    }

    queryCreatedInner.subscribe(
      KsqlSpecHelper
        .makeRowObserver(prefix = "streamStreamJoin", nextPlugin = Some(alertOnNullName))
        .toReactive(scheduler)
    )

    Thread.sleep(1000)
  }

  "join streams with repeating keys" in {}

  "join streams with different partition counts" in {}

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

    val query                                  = s"SELECT * FROM $orderShipmentJoinedStreamName EMIT CHANGES;"
    val joinedStreamQuery: StreamedQueryResult = client.streamQuery(query).get
    info(joinedStreamCreated)

    val detectMissingShipments: Row => Unit = (row: Row) => {
      val shipmentId: String = row.getString("SHIPMENTID")
      if (null == shipmentId) {
        warn(s"shipment is missing for: $row")
      }
    }
    // all orders have no shipments
    joinedStreamQuery.subscribe(
      KsqlSpecHelper.makeRowObserver(prefix = "orderShipmentJoin").toReactive(scheduler)
    )
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

    val query                                  = s"SELECT * FROM $orderShipmentJoinedStreamName EMIT CHANGES;"
    val joinedStreamQuery: StreamedQueryResult = client.streamQuery(query).get
    info(joinedStreamCreated)

    joinedStreamQuery.subscribe(
      KsqlSpecHelper.makeRowObserver(prefix = "orderShipmentJoin").toReactive(scheduler)
    )
    Thread.sleep(1000)
  }

  "windowed join - interactive" in {

    prepareOrdersAndShipments(false)

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

    // TODO - join on windowed streams (join ON window)

    val joinedStreamCreated: ExecuteStatementResult =
      client.executeStatement(orderShipmentJoinStreamSql).get

    val query                                  = s"SELECT * FROM $orderShipmentJoinedStreamName EMIT CHANGES;"
    val joinedStreamQuery: StreamedQueryResult = client.streamQuery(query).get
    info(joinedStreamCreated)

    joinedStreamQuery.subscribe(
      KsqlSpecHelper.makeRowObserver(prefix = "orderShipmentJoin").toReactive(scheduler)
    )

    val orderProducer =
      JsonStringProducer[String, Order](setup.commonProps, orderTopicName)

    val shipmentProducer =
      JsonStringProducer[String, Shipment](setup.commonProps, shipmentTopicName)

    val now: Long = System.currentTimeMillis()

    val order1 = ordersWithNewTimestamps.head.copy(timestamp = now)
    val order2 = order1.copy(id = "noShipment", timestamp = now)
    val shipment1 = shipmentsWithNewTimestamps
      .find(shipment => shipment.orderId == order1.id)
      .get
      .copy(id = "s1", timestamp = now) // - Duration.ofMinutes(5).toMillis)
    val shipment2 = shipment1.copy(id = "s2", timestamp = now + Duration.ofMinutes(5).toMillis)
    val shipment3 = shipment1.copy(id = "s3", timestamp = now - Duration.ofMinutes(5).toMillis)
    val shipment4 = shipment1.copy(id = "s4", timestamp = now + Duration.ofMinutes(5).toMillis)

    orderProducer.run(orderProducer.makeRecords(List(order1.id -> order1)))

    shipmentProducer.run(shipmentProducer.makeRecords(List(shipment1.id -> shipment1)))

    Thread.sleep(1000)
    // order join event happens before 1st order joins 1st shipment
    orderProducer.run(orderProducer.makeRecords(List(order2.id -> order2)))

    shipmentProducer.run(shipmentProducer.makeRecords(List(shipment2.id -> shipment2)))
    shipmentProducer.run(shipmentProducer.makeRecords(List(shipment3.id -> shipment3)))
    shipmentProducer.run(shipmentProducer.makeRecords(List(shipment4.id -> shipment4)))

    // one window per key per event
    // 1607595536230 - 1607595296230 = 4 minutes

    Thread.sleep(10000)
  }

  def prepareClicksAndUsers(produceTestData: Boolean = true): Unit = {
    KsqlSpecHelper.deleteTable(userTableName, client)
    KsqlSpecHelper.deleteStream(clickStreamName, client, adminClient)
    KsqlSpecHelper.deleteStream(leftJoinedStreamName, client, adminClient)
    KsqlSpecHelper.deleteStream(innerJoinedStreamName, client, adminClient)
    KsqlSpecHelper.deleteStream(userStreamName, client, adminClient)
    KsqlSpecHelper.deleteStream(userClicksStreamToStreamJoinName, client, adminClient)

    KsqlSpecHelper.deleteTopic(clickTopicName, adminClient)
    KsqlSpecHelper.deleteTopic(userTopicName, adminClient)

    KsqlSpecHelper.createTopic(clickTopicName, adminClient)
    KsqlSpecHelper.createTopic(userTopicName, adminClient)

    if (produceTestData) {
      val userProducer = JsonStringProducer[String, User](
        setup.commonProps,
        userTopicName,
        "userProducer" + Random.alphanumeric.take(10).mkString
      )
      val userRecords = userProducer.makeRecords((users map (d => d.id -> d)).toMap)
      userProducer.run(userRecords)

      val clickProducer = JsonStringProducer[String, Click](
        setup.commonProps,
        clickTopicName,
        "clickProducer" + Random.alphanumeric.take(10).mkString
      )
      val clickRecords = clickProducer.makeRecords((clicksFromKnownUsers map (d => d.userId -> d)))
      clickProducer.run(clickRecords)
    }

    val createClickStreamSql =
      s"CREATE STREAM $clickStreamName (userId VARCHAR KEY, element VARCHAR, userAgent VARCHAR, timestamp BIGINT ) WITH (kafka_topic='$clickTopicName', value_format='json', timestamp = 'timestamp');"

    val clickStreamCreated: ExecuteStatementResult =
      client.executeStatement(createClickStreamSql).get()
    info(clickStreamCreated)
  }

  def prepareOrdersAndShipments(produceTestData: Boolean = true): Unit = {

    KsqlSpecHelper.deleteStream(orderStreamName, client, adminClient)
    KsqlSpecHelper.deleteStream(shipmentStreamName, client, adminClient)
    KsqlSpecHelper.deleteStream(orderShipmentJoinedStreamName, client, adminClient)

    KsqlSpecHelper.deleteTopic(orderTopicName, adminClient)
    KsqlSpecHelper.deleteTopic(shipmentTopicName, adminClient)

    KsqlSpecHelper.createTopic(orderTopicName, adminClient)
    KsqlSpecHelper.createTopic(shipmentTopicName, adminClient)

    if (produceTestData) {
      val orderProducer =
        JsonStringProducer[String, Order](setup.commonProps, orderTopicName)
      val orderRecords =
        orderProducer.makeRecords((ordersWithNewTimestamps map (d => d.id -> d)).toMap)
      orderProducer.run(orderRecords)

      val shipmentProducer =
        JsonStringProducer[String, Shipment](setup.commonProps, shipmentTopicName)
      val shipmentRecords =
        shipmentProducer.makeRecords((shipmentsWithNewTimestamps map (d => d.id -> d)).toMap)
      shipmentProducer.run(shipmentRecords)
    }

    val createOrderStreamSql =
      s"CREATE STREAM $orderStreamName (id VARCHAR KEY, userId VARCHAR, prodId VARCHAR, amount INT, location VARCHAR, timestamp BIGINT ) WITH (kafka_topic='$orderTopicName', value_format='json', timestamp = 'timestamp');"

    val orderStreamCreated: ExecuteStatementResult =
      client.executeStatement(createOrderStreamSql).get()
    info(orderStreamCreated)

    val createShipmentStreamSql =
      s"CREATE STREAM $shipmentStreamName (id VARCHAR KEY, orderId VARCHAR, warehouse VARCHAR, timestamp BIGINT ) WITH (kafka_topic='$shipmentTopicName', value_format='json', timestamp = 'timestamp');"

    val shipmentStreamCreated: ExecuteStatementResult =
      client.executeStatement(createShipmentStreamSql).get()
    info(shipmentStreamCreated)
  }

}
