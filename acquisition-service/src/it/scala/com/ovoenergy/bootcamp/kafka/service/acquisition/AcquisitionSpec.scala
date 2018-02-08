package com.ovoenergy.bootcamp.kafka.service.acquisition

import akka.http.scaladsl.client.RequestBuilding
import com.ovoenergy.bootcamp.kafka.common.Randoms
import com.ovoenergy.bootcamp.kafka.domain.{Arbitraries, CreateAcquisition}
import com.whisk.docker.impl.dockerjava.DockerKitDockerJava
import com.whisk.docker.scalatest.DockerTestKit
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.concurrent.Eventually

import scala.collection.JavaConverters._


class AcquisitionSpec
  extends BaseIntegrationSpec
    with RequestBuilding
    with FailFastCirceSupport
    with Arbitraries
    with Randoms
    with DockerTestKit
    with DockerKitDockerJava
    with Eventually {

  type Key = String
  type Value = String

  val topic = "acquisition-topic"

  var consumer: Consumer[Key, Value] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    consumer = new KafkaConsumer[Key, Value](
      Map[String, AnyRef](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaEndpoint,
        ConsumerConfig.GROUP_ID_CONFIG -> "my-consumer-group-id",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
        ConsumerConfig.CLIENT_ID_CONFIG -> "KafkaConsumerSpec"
      ).asJava,
      new StringDeserializer,
      new StringDeserializer
    )
  }

  "AcquisitionService" when {
    "an acquisition is created" should {

      "reply with Created and acquisition details" in {
        val ca = random[CreateAcquisition]
        whenReady(createAcquisition(ca)){ acquisition =>
          acquisition.customerName shouldBe ca.customerName
        }
      }

      "produce a acquisition event" in {
        consumer.subscribe(Set(topic).asJava)

        val ca = random[CreateAcquisition]
        whenReady(createAcquisition(ca)){ acquisition =>

          eventually {
            val records = consumer.poll(500)
            records.count shouldBe 1

            val record = records.iterator().next()
            record.key() shouldBe acquisition.id.value
            record.value() shouldBe acquisition.toString
          }
        }
      }

    }
  }
}
