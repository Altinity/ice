package com.altinity.ice.benchmarks.simulations

import com.altinity.ice.benchmarks.{DatasetGenerator, IceRestProtocol}
import com.typesafe.config.ConfigFactory
import io.gatling.core.Predef._
import io.gatling.http.Predef._

import scala.concurrent.duration._

/**
 * 100% Write Workload: Creates a tree of namespaces, tables, and views.
 * 
 * This simulation creates a hierarchical dataset structure:
 * - N-ary tree of namespaces (width and depth configurable)
 * - Tables and views in each namespace
 * - Properties and schemas for each entity
 * 
 * Similar to Polaris CreateTreeDataset benchmark.
 */
class CreateTreeDatasetSimulation extends Simulation {
  
  private val config = ConfigFactory.load()
  private val workloadConfig = config.getConfig("workload.create-tree-dataset")
  
  private val namespaceConcurrency = workloadConfig.getInt("namespace-concurrency")
  private val tableConcurrency = workloadConfig.getInt("table-concurrency")
  private val viewConcurrency = workloadConfig.getInt("view-concurrency")
  
  private val catalogName = config.getString("dataset.catalog-name")
  
  // Generate dataset
  private val namespaces = DatasetGenerator.generateNamespaces()
  private val tables = DatasetGenerator.generateTables()
  private val views = DatasetGenerator.generateViews()
  
  println(s"Dataset configuration:")
  println(s"  Namespaces: ${namespaces.length}")
  println(s"  Tables: ${tables.length}")
  println(s"  Views: ${views.length}")
  println(s"  Total entities: ${namespaces.length + tables.length + views.length}")
  
  // Create namespace feeder
  private val namespaceFeeder = namespaces.map { ns =>
    Map(
      "namespace" -> ns.replace(".", "\u001F"), // Use unit separator for Iceberg REST API
      "properties" -> DatasetGenerator.propertiesToJson(
        DatasetGenerator.generateNamespaceProperties(ns)
      )
    )
  }.iterator
  
  // Create table feeder
  private val tableFeeder = tables.map { case (ns, tableName) =>
    Map(
      "namespace" -> ns.replace(".", "\u001F"),
      "tableName" -> tableName,
      "schema" -> DatasetGenerator.generateTableSchema(tableName),
      "properties" -> DatasetGenerator.propertiesToJson(
        DatasetGenerator.generateTableProperties(tableName)
      ),
      "location" -> s"${DatasetGenerator.defaultBaseLocation}$catalogName/$ns/$tableName"
    )
  }.iterator
  
  // Create view feeder
  private val viewFeeder = views.map { case (ns, viewName) =>
    Map(
      "namespace" -> ns.replace(".", "\u001F"),
      "viewName" -> viewName,
      "schema" -> DatasetGenerator.generateViewSchema(viewName),
      "properties" -> DatasetGenerator.propertiesToJson(
        DatasetGenerator.generateViewProperties(viewName)
      ),
      "sqlQuery" -> s"SELECT * FROM $ns.table_1"
    )
  }.iterator
  
  // Scenario: Create namespaces
  private val createNamespaces = scenario("Create Namespaces")
    .exec(IceRestProtocol.authenticate)
    .feed(namespaceFeeder)
    .exec(
      http("Create Namespace")
        .post("/v1/namespaces")
        .body(StringBody(
          """{"namespace": ["${namespace}"], "properties": ${properties}}"""
        )).asJson
        .check(status.in(200, 409)) // 409 = already exists
    )
  
  // Scenario: Create tables
  private val createTables = scenario("Create Tables")
    .feed(tableFeeder)
    .exec(
      http("Create Table")
        .post("/v1/namespaces/${namespace}/tables")
        .body(StringBody(
          """{
            "name": "${tableName}",
            "schema": ${schema},
            "location": "${location}",
            "properties": ${properties}
          }"""
        )).asJson
        .check(status.in(200, 409))
    )
  
  // Scenario: Create views
  private val createViews = scenario("Create Views")
    .feed(viewFeeder)
    .exec(
      http("Create View")
        .post("/v1/namespaces/${namespace}/views")
        .body(StringBody(
          """{
            "name": "${viewName}",
            "schema": ${schema},
            "view-version": {
              "version-id": 1,
              "schema-id": 0,
              "timestamp-ms": ${__time()},
              "summary": {"operation": "create"},
              "representations": [{
                "type": "sql",
                "sql": "${sqlQuery}",
                "dialect": "spark"
              }],
              "default-namespace": ["${namespace}"]
            },
            "properties": ${properties}
          }"""
        )).asJson
        .check(status.in(200, 409))
    )
  
  // Setup with authentication
  setUp(
    createNamespaces.inject(
      rampUsers(namespaces.length).during(30.seconds)
    ),
    createTables.inject(
      nothingFor(5.seconds),
      rampUsers(tables.length).during(60.seconds)
    ),
    createViews.inject(
      nothingFor(70.seconds),
      rampUsers(views.length).during(30.seconds)
    )
  ).protocols(IceRestProtocol.httpProtocol)
}

