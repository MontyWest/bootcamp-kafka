package com.ovoenergy.bootcamp.kafka.service.acquisition

import com.ovoenergy.comms.dockertestkit.{KafkaKit, SchemaRegistryKit}
import com.whisk.docker.{ContainerLink, DockerContainer, DockerKit, DockerReadyChecker}
import org.scalatest.concurrent.{Futures, ScalaFutures}

import scala.concurrent.Future
import scala.concurrent.duration._

trait AcquisitionServiceKit extends DockerKit {
  _: KafkaKit with SchemaRegistryKit with ScalaFutures =>

  val DefaultAcquisitionServicePort: Int = 8080

  def acquisitionServicePublishedPort: Int = acquisitionServiceContainer.getPorts()
    .flatMap { ports =>
      ports.get(DefaultAcquisitionServicePort)
        .fold(Future.failed[Int](new RuntimeException(s"the acquisition-service does not expose the $DefaultAcquisitionServicePort port ")))(Future.successful)
    }.futureValue(timeout(scaled(5.seconds)))

  def acquisitionServicePublicEndpoint = s"http://localhost:$acquisitionServicePublishedPort"

  lazy val acquisitionServiceContainer: DockerContainer =
    DockerContainer(s"kafka-workshop-acquisition-service:latest",
      Some("acquisition-service"))
      .withPorts(DefaultAcquisitionServicePort -> None)
      .withEnv(
        s"HTTP_PORT=$DefaultAcquisitionServicePort",
        s"KAFKA_ENDPOINT=$kafkaEndpoint",
        s"SCHEMA_REGISTRY_ENDPOINT=$schemaRegistryEndpoint"
      )
      .withReadyChecker(
        DockerReadyChecker
          .LogLineContains(s"Server online")
          .looped(10, 250.milliseconds)
      )

  abstract override def dockerContainers: List[DockerContainer] =
    acquisitionServiceContainer :: super.dockerContainers

}
