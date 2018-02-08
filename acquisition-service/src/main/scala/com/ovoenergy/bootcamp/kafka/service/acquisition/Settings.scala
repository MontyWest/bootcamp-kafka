package com.ovoenergy.bootcamp.kafka.service.acquisition

import akka.http.scaladsl.model.Uri

case class Settings(httpHost: String,
                    httpPort: Int,
                    kafkaEndpoint: String)
