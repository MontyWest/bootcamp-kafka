package com.ovoenergy.bootcamp.kafka.service.acquisition

import com.ovoenergy.comms.dockertestkit.{KafkaKit, SchemaRegistryKit}
import com.whisk.docker.{DockerContainer, DockerKit, DockerReadyChecker}
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Future
import scala.concurrent.duration._

trait CustomerServiceKit extends DockerKit {
  _: KafkaKit with SchemaRegistryKit with ScalaFutures =>

  val DefaultCustomerServicePort: Int = 8080

  def customerServicePublishedPort: Int = customerServiceContainer.getPorts()
    .flatMap { ports =>
      ports.get(DefaultCustomerServicePort)
        .fold(Future.failed[Int](new RuntimeException(s"the customer-service does not expose the $DefaultCustomerServicePort port ")))(Future.successful)
    }.futureValue(timeout(scaled(5.seconds)))

  def customerServicePublicEndpoint = s"http://localhost:$customerServicePublishedPort"

  lazy val customerServiceContainer: DockerContainer =
    DockerContainer(s"kafka-workshop-customer-service:latest", Some("customer-service"))
      .withPorts(DefaultCustomerServicePort -> None)
      .withEnv(
        s"HTTP_PORT=$DefaultCustomerServicePort",
        s"KAFKA_ENDPOINT=$kafkaEndpoint",
        s"SCHEMA_REGISTRY_ENDPOINT=$schemaRegistryEndpoint"
      )
      .withReadyChecker(
        DockerReadyChecker
          .LogLineContains(s"Server online")
          .looped(10, 250.milliseconds)
      )

  abstract override def dockerContainers: List[DockerContainer] =
    customerServiceContainer :: super.dockerContainers

}
