package com.ovoenergy.bootcamp.kafka.service.acquisition

import java.time.LocalDateTime

import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.ovoenergy.bootcamp.kafka.domain.Account.AccountId
import com.ovoenergy.bootcamp.kafka.domain.Customer.CustomerId
import com.ovoenergy.bootcamp.kafka.domain._
import com.ovoenergy.bootcamp.kafka.service.acquisition.AcquisitionService.AcquisitionRepository
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import io.circe.Json
import org.scalatest.{FunSuite, Matchers, WordSpec}

import scala.concurrent.Future

class AcquisitionServiceSpec extends  WordSpec with Matchers with RequestBuilding with ScalatestRouteTest {

  def mockCreateCustomer(cc: CreateCustomer): Future[Customer] = Future.successful {
    Customer(CustomerId.unique(), cc.acquisitionId, cc.name, cc.emailAddress, LocalDateTime.now())
  }

  def mockCreateAccount(ca: CreateAccount): Future[Account] = Future.successful {
    Account(AccountId.unique(), ca.acquisitionId, ca.tariff, ca.domicileAddress, ca.billingAddress)
  }

  def mockProduceAcquisition(acq: Acquisition): Future[Acquisition] = Future.successful(acq)

  "AcquisitionService" when {
    "receive a Get request to /admin/ping" should {
      "return 200 with Pong body" in {
        Get("/admin/ping") ~> AcquisitionService.routes(new AcquisitionRepository, mockProduceAcquisition) ~> check {
          status shouldBe StatusCodes.OK
          responseAs[String] shouldBe "pong"
        }
      }
    }
  }

}
