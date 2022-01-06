package com.example.ksqldb.util

import io.confluent.ksql.api.client.{ Client, ClientOptions }
import org.apache.kafka.clients.admin.{ AdminClient, AdminClientConfig }

import java.util.Properties

case class LocalSetup(host: String = "localhost", ksqlDbPort: Int = 8088, brokerPort: Int = 29092) extends KsqlConnectionSetup {

  private val options: ClientOptions = ClientOptions
    .create()
    .setHost(host)
    .setPort(ksqlDbPort)
  val client: Client = Client.create(options)

  val clientBootstrapServer = s"$host:$brokerPort"
  val commonProps   = new Properties()
  commonProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, clientBootstrapServer)
  val adminClient: AdminClient = AdminClient.create(commonProps)
}
