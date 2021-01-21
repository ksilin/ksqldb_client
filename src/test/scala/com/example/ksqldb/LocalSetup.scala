package com.example.ksqldb

import io.confluent.ksql.api.client.{Client, ClientOptions}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}

import java.util.Properties

case class LocalSetup(host: String = "localhost", ksqlDbPort: Int = 8088, brokerPort: Int = 29092) {

  private val options: ClientOptions = ClientOptions
    .create()
    .setHost(host)
    .setPort(ksqlDbPort)
  val client: Client = Client.create(options)

  val adminClientBootstrapServer = s"$host:$brokerPort"
  private val adminClientProps      = new Properties()
  adminClientProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, adminClientBootstrapServer)
  val adminClient: AdminClient = AdminClient.create(adminClientProps)
}
