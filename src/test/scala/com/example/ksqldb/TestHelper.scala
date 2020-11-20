package com.example.ksqldb

import java.util.Collections

import io.confluent.ksql.api.client.{Client, ExecuteStatementResult, QueryInfo, SourceDescription, StreamInfo}
import org.apache.kafka.clients.admin.{AdminClient, CreateTopicsResult, NewTopic}
import org.apache.kafka.common.config.TopicConfig

import scala.jdk.CollectionConverters._

import scala.collection.mutable

object TestHelper {


  def createTopic(adminClient: AdminClient, topicName: String, numberOfPartitions: Int, replicationFactor: Short): Unit = {
    if (!adminClient.listTopics().names().get().contains(topicName)) {
      println("Creating topic {}", topicName)

      val configs: Map[String, String] = if(replicationFactor < 3) Map(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG -> "1") else Map.empty

      val newTopic: NewTopic = new NewTopic(topicName, numberOfPartitions, replicationFactor)
      newTopic.configs(configs.asJava)
      try {
        val topicsCreationResult: CreateTopicsResult = adminClient.createTopics(Collections.singleton(newTopic))
        topicsCreationResult.all().get()
      } catch {
        case e: Throwable => println(e)
      }
    } else {
      println(s"topic $topicName already exists, skipping")
    }
  }

  def deleteTopic(topicName: String, adminClient: AdminClient): Any = {
    println(s"deleting topic $topicName")
    try {
      val topicDeletionResult = adminClient.deleteTopics(List(topicName).asJava)
      topicDeletionResult.all().get()
    } catch {
      case e: Throwable => println(e)
    }
  }

  def deleteStream(streamName: String, client: Client, adminClient: AdminClient, deleteSinkTopic: Boolean = true): Unit = {
    val streams: mutable.Buffer[StreamInfo] = client.listStreams().get().asScala
    streams.find(_.getName.equalsIgnoreCase(streamName)).foreach { streamToDelete =>
      println(s"found existing stream: $streamToDelete")

      if(deleteSinkTopic) {
        val topicName: String = streamToDelete.getTopic
        println(s"deleting sink topic: $topicName")
        deleteTopic(topicName, adminClient)
      }

      // find the queries listening to this stream:
      val queries: mutable.Seq[QueryInfo] = client.listQueries().get().asScala
      // println("existing queries: ")
      // queries foreach println

      val relevantQueries = queries.filter(_.getSql.toUpperCase.contains(streamName.toUpperCase))

      println("queries for stream: ")
      relevantQueries foreach println

      relevantQueries.foreach{ q =>
        println(s"terminating ${q.getId}")
        val res: ExecuteStatementResult = client.executeStatement(s"TERMINATE ${q.getId};").get()
        println(res)
      }

      val streamDeleted: ExecuteStatementResult = client.executeStatement(s"DROP STREAM IF EXISTS $streamName;").get()
      println(s"stream $streamName deleted: $streamDeleted")
    }
  }

  def deleteTable(tableName: String, client: Client): Unit = {
    println(s"deleting table $tableName")
    client.listTables().get.asScala.find(t => t.getName.equalsIgnoreCase(tableName)).foreach { t =>
      try {client.executeStatement(s"DROP TABLE IF EXISTS $tableName;").get} catch {
        case e: Throwable => println(s"failed to drop table $tableName: $e")
      }
    }
  }

  def printSourceDescription(sd: SourceDescription): Unit = {
    println("name: " + sd.name())
    println("type: " + sd.`type`())
    println("topic: " + sd.topic())
    println("keyFormat: " + sd.keyFormat())
    println("valueFormat: " + sd.valueFormat())
    println("timestampColumn: " + sd.timestampColumn())
    println("windowType: " + sd.windowType())
    println("fields: " + sd.fields())
    println("sqlStatement: " + sd.sqlStatement())
    println("readQueries: " + sd.readQueries())
    println("writeQueries: " + sd.writeQueries())
  }

}