package com.example.ksqldb.util

import io.confluent.ksql.api.client.{Client, ClientOptions}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}

import java.util.Properties

case class CCloudSetup(ksqlHost: String, ksqlDbPort: Int = 8088, clientProps: ClientProps) extends KsqlConnectionSetup {

  private val options: ClientOptions = ClientOptions
    .create()
    .setHost(ksqlHost)
    .setPort(ksqlDbPort)
  val client: Client = Client.create(options)
  val commonProps: Properties = clientProps.clientProps
  val adminClient: AdminClient = AdminClient.create(commonProps)
}
