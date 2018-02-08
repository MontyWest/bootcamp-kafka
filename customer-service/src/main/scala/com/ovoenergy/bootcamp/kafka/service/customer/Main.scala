package com.ovoenergy.bootcamp.kafka.service.customer

import java.time.LocalDateTime

import akka.http.scaladsl.server.{HttpApp, Route}
import ciris.syntax._
import ciris.{env, loadConfig, prop}
import com.ovoenergy.bootcamp.kafka.domain.Customer.CustomerId
import com.ovoenergy.bootcamp.kafka.domain.{Acquisition, Customer}
import com.ovoenergy.bootcamp.kafka.service.customer.CustomerService.CustomerRepository
import com.ovoenergy.kafka.serialization.avro4s._
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._
import scala.concurrent.Future


object Main extends HttpApp with App {

  type Key = String
  type Value = Acquisition
  val topic = "acquisition"

  val repo = new CustomerRepository

  val settings: Settings = loadConfig(
    env[Option[String]]("HTTP_HOST").orElse(prop[Option[String]]("http.host")),
    env[Option[Int]]("HTTP_PORT").orElse(prop[Option[Int]]("http.port")),
    env[String]("KAFKA_ENDPOINT").orElse(prop[String]("kafka.endpoint")),
    env[String]("SCHEMA_REGISTRY_ENDPOINT").orElse(prop[String]("schema-registry.endpoint"))
  )((host, port, kafkaEndpoint, schemaRegistryEndpoint) =>
    Settings(host.getOrElse("0.0.0.0"), port.getOrElse(8080), kafkaEndpoint = kafkaEndpoint, schemaRegistryEndpoint = schemaRegistryEndpoint)).orThrow()


  val consumer = new KafkaConsumer[Key, Value](
    Map[String, AnyRef](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> settings.kafkaEndpoint,
      ConsumerConfig.GROUP_ID_CONFIG -> "customer-group-id",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
      ConsumerConfig.CLIENT_ID_CONFIG -> "CustomerService"
    ).asJava,
    new StringDeserializer,
    avroBinarySchemaIdDeserializer[Value](settings.schemaRegistryEndpoint, isKey = false, includesFormatByte = true)
  )

  override protected def routes: Route = CustomerService.routes(repo)

  consumer.subscribe(Set(topic).asJava)



  new Thread() {
    override def run(): Unit =
      Iterator
        .continually(consumer.poll(150))
        .foreach { records =>
          records.iterator().asScala foreach { record =>
            println(s"DEBUG: Received record from acquisitions: $records")
            val ac: Acquisition = record.value()
            val customer = Customer(CustomerId.unique(),
              ac.id,
              ac.customerName,
              ac.customerEmailAddress,
              LocalDateTime.now())
            repo.put(customer.id, customer)
          }
          consumer.commitSync()
        }
  }.start()

  startServer(settings.httpHost, settings.httpPort)



}
