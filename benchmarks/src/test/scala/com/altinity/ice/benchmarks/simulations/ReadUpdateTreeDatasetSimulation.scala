package com.altinity.ice.benchmarks.simulations

import com.altinity.ice.benchmarks.{DatasetGenerator, IceRestProtocol}
import com.typesafe.config.ConfigFactory
import io.gatling.core.Predef._
import io.gatling.http.Predef._

import scala.concurrent.duration._
import scala.util.Random

/**
 * Mixed Read/Write Workload: 90% reads, 10% writes (configurable).
 * 
 * This simulation exercises various read and write operations:
 * - Read: namespace exists, table exists, load table, list tables, etc.
 * - Write: update namespace properties, update table metadata
 * 
 * Similar to Polaris ReadUpdateTreeDataset benchmark.
 */
class ReadUpdateTreeDatasetSimulation extends Simulation {
  
  private val config = ConfigFactory.load()
  private val workloadConfig = config.getConfig("workload.read-update-tree-dataset")
  
  private val readWriteRatio = workloadConfig.getDouble("read-write-ratio")
  private val throughput = workloadConfig.getInt("throughput")
  private val durationMinutes = workloadConfig.getInt("duration-in-minutes")
  
  // Generate dataset
  private val namespaces = DatasetGenerator.generateNamespaces()
  private val tables = DatasetGenerator.generateTables()
  private val views = DatasetGenerator.generateViews()
  
  private val rnd = new Random(42) // Fixed seed for reproducibility
  
  println(s"Mixed workload configuration:")
  println(s"  Read/Write ratio: ${readWriteRatio * 100}% / ${(1 - readWriteRatio) * 100}%")
  println(s"  Target throughput: $throughput ops/s")
  println(s"  Duration: $durationMinutes minutes")
  println(s"  Total operations: ${throughput * durationMinutes * 60}")
  
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
  
  private val viewFeeder = Iterator.continually {
    val (ns, viewName) = views(rnd.nextInt(views.length))
    Map(
      "namespace" -> ns.replace(".", "\u001F"),
      "viewName" -> viewName
    )
  }
  
  // Read operations
  private val readOps = Seq(
    exec(
      http("Read / Check Namespace Exists")
        .head("/v1/namespaces/${namespace}")
        .check(status.in(200, 204, 404))
    ),
    exec(
      http("Read / Fetch Namespace")
        .get("/v1/namespaces/${namespace}")
        .check(status.in(200, 404))
    ),
    exec(
      http("Read / List Tables")
        .get("/v1/namespaces/${namespace}/tables")
        .check(status.in(200, 404))
    ),
    exec(
      http("Read / Check Table Exists")
        .head("/v1/namespaces/${namespace}/tables/${tableName}")
        .check(status.in(200, 204, 404))
    ),
    exec(
      http("Read / Fetch Table")
        .get("/v1/namespaces/${namespace}/tables/${tableName}")
        .check(status.in(200, 404))
    ),
    exec(
      http("Read / List Views")
        .get("/v1/namespaces/${namespace}/views")
        .check(status.in(200, 404))
    ),
    exec(
      http("Read / Check View Exists")
        .head("/v1/namespaces/${namespace}/views/${viewName}")
        .check(status.in(200, 204, 404))
    ),
    exec(
      http("Read / Fetch View")
        .get("/v1/namespaces/${namespace}/views/${viewName}")
        .check(status.in(200, 404))
    )
  )
  
  // Write operations
  private val writeOps = Seq(
    exec(
      http("Write / Update Namespace Properties")
        .post("/v1/namespaces/${namespace}/properties")
        .body(StringBody(
          s"""{"updates": {"updated_at": "$${__time()}"}}"""
        )).asJson
        .check(status.in(200, 404))
    ),
    exec(
      http("Write / Update Table Metadata")
        .post("/v1/namespaces/${namespace}/tables/${tableName}")
        .body(StringBody(
          """{
            "requirements": [],
            "updates": [{
              "action": "set-properties",
              "updates": {"updated_at": "${__time()}"}
            }]
          }"""
        )).asJson
        .check(status.in(200, 404))
    )
  )
  
  // Mixed scenario
  private val mixedWorkload = scenario("Mixed Read/Write Workload")
    .exec(IceRestProtocol.authenticate)
    .during(durationMinutes.minutes) {
      randomSwitch(
        readWriteRatio -> group("Read")(
          feed(namespaceFeeder)
            .feed(tableFeeder)
            .feed(viewFeeder)
            .exec(readOps(rnd.nextInt(readOps.length)))
        ),
        (1 - readWriteRatio) -> group("Write")(
          feed(namespaceFeeder)
            .feed(tableFeeder)
            .exec(writeOps(rnd.nextInt(writeOps.length)))
        )
      )
    }
  
  setUp(
    mixedWorkload.inject(
      constantUsersPerSec(throughput).during(durationMinutes.minutes)
    )
  ).protocols(IceRestProtocol.httpProtocol)
}

