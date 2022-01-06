package com.example.ksqldb

import java.util.Properties
import org.apache.kafka.clients.producer.{
  KafkaProducer,
  ProducerConfig,
  ProducerRecord,
  RecordMetadata
}
import io.circe._
import io.circe.syntax._
import org.apache.kafka.common.serialization.StringSerializer
import wvlet.log.LogSupport

case class JsonStringProducer[K, V](
    clientProperties: Properties,
    topic: String = "testTopic",
    clientId: String = "JsonStringProducer"
)(implicit e: Encoder[V])
    extends LogSupport {

  val producerProperties = new Properties()
  producerProperties.putAll(clientProperties)
  producerProperties.put(ProducerConfig.ACKS_CONFIG, "all")
  producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
  producerProperties.put(
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    classOf[StringSerializer]
  ) // so, not really K
  producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, clientId)

  val producer = new KafkaProducer[K, String](producerProperties)

  def makeRecords(recordMap: Iterable[(K, V)]): Iterable[ProducerRecord[K, String]] =
    recordMap.map { case (k, v) => makeRecord(k, v) }

  def makeRecord(key: K, value: V): ProducerRecord[K, String] =
    new ProducerRecord[K, String](topic, key, value.asJson.noSpaces)

  def run(r: ProducerRecord[K, String]): Unit = {
    info(s"producing $r")
    val res: RecordMetadata = producer.send(r).get
    info(s"produced ${res.topic()}, | ${res.partition()} | ${res.offset()} | ${res.timestamp()}")
  }

  def run(msgs: Iterable[ProducerRecord[K, String]], sendDelayMs: Int = 0): Unit =
    msgs foreach { r =>
      run(r)
      Thread.sleep(sendDelayMs)
    }

}
