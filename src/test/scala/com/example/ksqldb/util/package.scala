package com.example.ksqldb

import io.confluent.ksql.api.client.Client
import org.apache.kafka.clients.admin.AdminClient

import java.util.Properties

package object util {

  abstract class KsqlConnectionSetup {
    def client: Client
    def commonProps: Properties
    def adminClient: AdminClient
  }

}
