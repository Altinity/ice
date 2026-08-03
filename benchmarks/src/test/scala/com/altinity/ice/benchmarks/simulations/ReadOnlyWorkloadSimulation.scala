package com.altinity.ice.benchmarks.simulations

import com.altinity.ice.benchmarks.{DatasetGenerator, IceRestProtocol}
import com.typesafe.config.ConfigFactory
import io.gatling.core.Predef._
import io.gatling.http.Predef._

import scala.concurrent.duration._
import scala.util.Random

/**
 * 100% Read Workload: Exercises read-only operations on the catalog.
 * 
 * This simulation tests read performance with various operations:
 * - List namespaces/tables/views
 * - Load table/view metadata
 * - Check entity existence
 * 
 * Useful for testing catalog read scalability and caching effectiveness.
 */
class ReadOnlyWorkloadSimulation extends Simulation {
  
  private val config = ConfigFactory.load()
  private val workloadConfig = config.getConfig("workload.read-only-workload")
  
  private val throughput = workloadConfig.getInt("throughput")
  private val durationMinutes = workloadConfig.getInt("duration-in-minutes")
  
  // Generate dataset
  private val namespaces = DatasetGenerator.generateNamespaces()
  private val tables = DatasetGenerator.generateTables()
  private val views = DatasetGenerator.generateViews()
  
  private val rnd = new Random(42)
  
  println(s"Read-only workload configuration:")
  println(s"  Target throughput: $throughput ops/s")
  println(s"  Duration: $durationMinutes minutes")
  
  // Create feeders
  private val namespaceFeeder = Iterator.continually {
    val ns = namespaces(rnd.nextInt(namespaces.length))
    Map("namespace" -> ns.replace(".", "\u001F"))
  }
  
  private val tableFeeder = Iterator.continually {
    val (ns, tableName) = tables(rnd.nextInt(tables.length))
    Map(
      "namespace" -> ns.replace(".", "\u001F"),
      "tableName" -> tableName
    )
  }
  
  // Read-only scenario
  private val readOnlyWorkload = scenario("Read-Only Workload")
    .exec(IceRestProtocol.authenticate)
    .during(durationMinutes.minutes) {
      feed(namespaceFeeder)
        .feed(tableFeeder)
        .randomSwitch(
          20.0 -> exec(
            http("List Namespaces")
              .get("/v1/namespaces")
              .check(status.is(200))
          ),
          20.0 -> exec(
            http("List Tables")
              .get("/v1/namespaces/${namespace}/tables")
              .check(status.in(200, 404))
          ),
          20.0 -> exec(
            http("Check Table Exists")
              .head("/v1/namespaces/${namespace}/tables/${tableName}")
              .check(status.in(200, 204, 404))
          ),
          30.0 -> exec(
            http("Load Table")
              .get("/v1/namespaces/${namespace}/tables/${tableName}")
              .check(status.in(200, 404))
          ),
          10.0 -> exec(
            http("Load Namespace")
              .get("/v1/namespaces/${namespace}")
              .check(status.in(200, 404))
          )
        )
    }
  
  setUp(
    readOnlyWorkload.inject(
      constantUsersPerSec(throughput).during(durationMinutes.minutes)
    )
  ).protocols(IceRestProtocol.httpProtocol)
}

