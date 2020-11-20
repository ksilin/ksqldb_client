package com.example.ksqldb

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord, RecordMetadata}
import io.circe._
import io.circe.syntax._
import monix.execution.Scheduler.Implicits.global
import monix.eval._
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.immutable

case class JsonStringProducer[K, V](bootstrapServers: String = "localhost:9092", topic: String = "testTopic")(implicit e: Encoder[V]) {

  val producerProperties = new Properties()
  producerProperties.put(ProducerConfig.ACKS_CONFIG, "all")
  producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
  producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer]) // so, not really K
  producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "JsonStringProducer")

  val producer = new KafkaProducer[K, String](producerProperties)

  def makeRecords(recordMap: Map[K, V] ): immutable.Iterable[ProducerRecord[K, String]] = {
    recordMap.map{ case (k,v) => new ProducerRecord[K, String](topic, k, v.asJson.noSpaces)}
  }

  def run(msgs: Iterable[ProducerRecord[K, String]],  sendDelayMs: Int = 100): Unit = {
    msgs foreach { r =>
      println(s"producing $r")
      val res: RecordMetadata = producer.send(r).get
      println(s"${res.topic()}, | ${res.partition()} | ${res.offset()} | ${res.timestamp()}")
      Thread.sleep(sendDelayMs)
    }
  }






}
