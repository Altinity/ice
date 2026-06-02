package com.altinity.ice.benchmarks

import com.typesafe.config.{Config, ConfigFactory}
import io.gatling.core.Predef._
import io.gatling.http.Predef._
import io.gatling.http.protocol.HttpProtocolBuilder

import scala.concurrent.duration._

/**
 * Protocol configuration for ICE REST Catalog benchmarks.
 * Handles bearer token authentication and HTTP settings.
 */
object IceRestProtocol {
  
  private val config: Config = ConfigFactory.load()
  
  val baseUrl: String = config.getString("http.base-url")
  val bearerToken: String = config.getString("auth.bearer-token")
  val connectTimeout: Duration = config.getInt("http.connect-timeout").milliseconds
  val requestTimeout: Duration = config.getInt("http.request-timeout").milliseconds
  
  /**
   * Sets the bearer token in the session.
   * This must be called at the start of each scenario.
   */
  val authenticate = exec(session => {
    session.set("accessToken", bearerToken)
  })
  
  /**
   * Returns the HTTP protocol configuration for ICE REST catalog.
   */
  def httpProtocol: HttpProtocolBuilder = {
    val protocol = http
      .baseUrl(baseUrl)
      .acceptHeader("application/json")
      .contentTypeHeader("application/json")
      .shareConnections
    
    // Only add Authorization header if bearer token is provided
    if (bearerToken.nonEmpty) {
      protocol.header("Authorization", "Bearer ${accessToken}")
    } else {
      protocol
    }
  }
}

