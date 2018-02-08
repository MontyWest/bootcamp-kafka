package com.ovoenergy.bootcamp.kafka.service.acquisition

import com.ovoenergy.comms.dockertestkit.{KafkaKit, SchemaRegistryKit}
import com.whisk.docker.{DockerContainer, DockerKit, DockerReadyChecker}
import org.scalatest.concurrent.ScalaFutures
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration._

trait AccountServiceKit extends DockerKit {
  _: KafkaKit with SchemaRegistryKit with ScalaFutures =>

  val DefaultAccountServicePort: Int = 8080

  def accountServicePublishedPort: Int = accountServiceContainer.getPorts()
    .flatMap { ports =>
      ports.get(DefaultAccountServicePort)
        .fold(Future.failed[Int](new RuntimeException(s"the account-service does not expose the $DefaultAccountServicePort port ")))(Future.successful)
    }.futureValue(timeout(scaled(5.seconds)))

  def accountServicePublicEndpoint = s"http://localhost:$accountServicePublishedPort"

  lazy val accountServiceContainer: DockerContainer =
    DockerContainer(s"kafka-workshop-account-service:latest", Some("account-service"))
      .withPorts(DefaultAccountServicePort -> None)
      .withEnv(
        s"HTTP_PORT=$DefaultAccountServicePort",
        s"KAFKA_ENDPOINT=$kafkaEndpoint",
        s"SCHEMA_REGISTRY_ENDPOINT=$schemaRegistryEndpoint")
      .withReadyChecker(
        DockerReadyChecker
          .LogLineContains(s"Server online")
          .looped(10, 250.milliseconds)
      )

  abstract override def dockerContainers: List[DockerContainer] =
    accountServiceContainer :: super.dockerContainers

}
