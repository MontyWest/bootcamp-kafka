package com.ovoenergy.bootcamp.kafka.service.acquisition

import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import com.ovoenergy.bootcamp.kafka.domain.{Account, Acquisition, CreateAcquisition, Customer}
import com.whisk.docker.impl.dockerjava.DockerKitDockerJava
import com.whisk.docker.scalatest.DockerTestKit
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Assertion, Matchers, WordSpec}
import com.ovoenergy.bootcamp.kafka.common.serde._
import com.ovoenergy.comms.dockertestkit.{KafkaKit, SchemaRegistryKit, ZookeeperKit}
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport

import scala.concurrent.Future
import scala.concurrent.duration._

abstract class BaseIntegrationSpec
    extends WordSpec
    with Matchers
    with ScalaFutures
    with ActorSystemFixture
    with MaterializerFixture
    with DockerTestKit
    with DockerKitDockerJava
    with CustomerServiceKit
    with AccountServiceKit
    with AcquisitionServiceKit
    with ZookeeperKit
    with KafkaKit
    with SchemaRegistryKit
    with RequestBuilding
    with FailFastCirceSupport {

  implicit override def patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(10.seconds), interval = 250.milliseconds)

  def createAcquisition(
      createAcquisition: CreateAcquisition): Future[Acquisition] = {
    for {
      response <- Http().singleRequest(
        Post(s"$acquisitionServicePublicEndpoint/api/v1/acquisition",
             createAcquisition))
      if response.status.isSuccess()
      acquisition <- Unmarshal(response).to[Acquisition]
    } yield acquisition
  }

  def fetchCustomers(): Future[Seq[Customer]] = {
    for {
      response <- Http().singleRequest(
        Get(s"$customerServicePublicEndpoint/api/v1/customer"))
      if response.status.isSuccess()
      customers <- Unmarshal(response).to[Seq[Customer]]
    } yield customers
  }

  def fetchAccounts(): Future[Seq[Account]] = {
    for {
      response <- Http().singleRequest(
        Get(s"$accountServicePublicEndpoint/api/v1/account"))
      if response.status.isSuccess()
      accounts <- Unmarshal(response).to[Seq[Account]]
    } yield accounts
  }

  def pingService(endpoint: String): Assertion = {
    withClue(endpoint) {
      whenReady(for {
        response <- Http().singleRequest(Get(s"$endpoint/admin/ping"))
        if response.status.isSuccess()
        stringBody <- Unmarshaller.stringUnmarshaller(response.entity)
      } yield stringBody) { stringBody =>
        stringBody shouldBe "pong"
      }
    }
  }
}
