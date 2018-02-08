package com.ovoenergy.bootcamp.kafka.service.acquisition

import akka.http.scaladsl.client.RequestBuilding
import com.ovoenergy.bootcamp.kafka.common.Randoms
import com.ovoenergy.bootcamp.kafka.domain.{Account, Arbitraries, CreateAcquisition, Customer}
import com.ovoenergy.comms.dockertestkit.{KafkaKit, ZookeeperKit}
import com.whisk.docker.impl.dockerjava.DockerKitDockerJava
import com.whisk.docker.scalatest.DockerTestKit
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer, StringSerializer}
import org.scalatest.concurrent.Eventually

import scala.collection.JavaConverters._

class AcquisitionEventSpec
    extends BaseIntegrationSpec
    with RequestBuilding
    with FailFastCirceSupport
    with Arbitraries
    with Randoms
    with Eventually {

  type Key = String
  type Value = Array[Byte]

  var consumer: Consumer[Key, Value] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    consumer = new KafkaConsumer[Key, Value](
      Map[String, AnyRef](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaEndpoint,
        ConsumerConfig.GROUP_ID_CONFIG -> "my-consumer-group-id",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
        ConsumerConfig.CLIENT_ID_CONFIG -> "KafkaConsumerSpec"
      ).asJava,
      new StringDeserializer,
      new ByteArrayDeserializer
    )

  }

  "AcquisitionService" when {
    "an acquisition is created" should {

      "reply with Created and acquisition details" in {
        val ca = random[CreateAcquisition]
        whenReady(createAcquisition(ca)) { acquisition =>
          acquisition.customerName shouldBe ca.customerName
        }
      }

      "produce a kafka event" in {
        whenReady(createAcquisition(random[CreateAcquisition])) {_ =>
          consumer.subscribe(Set("acquisition").asJava)
          eventually {
            consumer.poll(500).asScala.toList should not be empty
          }
        }
      }
    }
  }

}
