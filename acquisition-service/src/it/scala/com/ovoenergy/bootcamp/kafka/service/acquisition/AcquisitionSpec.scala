package com.ovoenergy.bootcamp.kafka.service.acquisition

import akka.http.scaladsl.client.RequestBuilding
import com.ovoenergy.bootcamp.kafka.common.Randoms
import com.ovoenergy.bootcamp.kafka.domain.{Account, Arbitraries, CreateAcquisition, Customer}
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import org.scalatest.concurrent.Eventually

class AcquisitionSpec
  extends BaseIntegrationSpec
    with RequestBuilding
    with FailFastCirceSupport
    with Arbitraries
    with Randoms
    with Eventually {

  "AcquisitionService" when {
    "an acquisition is created" should {

      "reply with Created and acquisition details" in {
        val ca = random[CreateAcquisition]
        whenReady(createAcquisition(ca)){ acquisition =>
          acquisition.customerName shouldBe ca.customerName
        }
      }

      "create a Customer" in {
        whenReady(
          for {
            acquisition <- createAcquisition(random[CreateAcquisition])
            customers <- fetchCustomers()
          } yield acquisition->customers
        ){case (acquisition, customers) =>
          eventually {
            customers.find(_.acquisitionId == acquisition.id) shouldBe a[Some[Customer]]
          }
        }
      }

      "create an Account" in {

        whenReady(
          for {
            acquisition <- createAcquisition(random[CreateAcquisition])
            accounts <- fetchAccounts()
          } yield acquisition->accounts
        ){case (acquisition, accounts) =>
          eventually {
            accounts.find(_.acquisitionId == acquisition.id) shouldBe a[Some[Account]]
          }
        }
      }
    }
  }



}
