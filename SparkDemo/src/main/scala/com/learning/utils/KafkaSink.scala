package com.learning.utils

/**
 * @Author: tanggaomeng
 * @Date: 2021/7/9 15:55
 * @Description:
 * @Version 1.0
 */

import java.util.concurrent.Future
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import scala.collection.JavaConversions._

class KafkaSink[K, V](createProducer: () => KafkaProducer[K, V]) extends Serializable {
    lazy val producer: KafkaProducer[K, V] = createProducer()

    def send(topic: String, key: K, value: V): Future[RecordMetadata] =
        producer.send(new ProducerRecord[K, V](topic, key, value))

    def send(topic: String, value: V): Future[RecordMetadata] =
        producer.send(new ProducerRecord[K, V](topic, value))
}

object KafkaSink {

    def apply[K, V](config: Map[String, Object]): KafkaSink[K, V] = {
        val createProducerFunc: () => KafkaProducer[K, V] = () => {
            val producer = new KafkaProducer[K, V](config)
            sys.addShutdownHook {
                producer.close()
            }
            producer
        }
        new KafkaSink(createProducerFunc)
    }

    def apply[K, V](config: java.util.Properties): KafkaSink[K, V] = apply(config.toMap)

}