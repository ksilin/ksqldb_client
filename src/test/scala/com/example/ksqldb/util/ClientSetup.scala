package com.example.ksqldb.util

import org.apache.kafka.clients.admin.AdminClient

import java.net.URL
import java.util.Properties

case class ClientSetup(configFileUrl: Option[URL] = None, configPath: Option[String] = None) {

  val ksqlClientProps: KsqlClientProps =
    KsqlClientProps.ksqlClientPropsFromConfigFile(configFileUrl, configPath)
  val clientProps              = CCloudClientProps.create(configFileUrl, configPath)
  val commonProps: Properties  = clientProps.clientProps
  val adminClient: AdminClient = AdminClient.create(commonProps)
}
