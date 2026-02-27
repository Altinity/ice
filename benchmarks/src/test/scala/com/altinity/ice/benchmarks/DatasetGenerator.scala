package com.altinity.ice.benchmarks

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Random

/**
 * Procedural dataset generator for ICE REST Catalog benchmarks.
 * Given the same configuration, generates the same dataset for reproducible benchmarks.
 */
object DatasetGenerator {
  
  private val config: Config = ConfigFactory.load().getConfig("dataset.tree")
  
  val numCatalogs: Int = config.getInt("num-catalogs")
  val namespaceWidth: Int = config.getInt("namespace-width")
  val namespaceDepth: Int = config.getInt("namespace-depth")
  val tablesPerNamespace: Int = config.getInt("tables-per-namespace")
  val viewsPerNamespace: Int = config.getInt("views-per-namespace")
  val namespaceProperties: Int = config.getInt("namespace-properties")
  val tableProperties: Int = config.getInt("table-properties")
  val viewProperties: Int = config.getInt("view-properties")
  val columnsPerTable: Int = config.getInt("columns-per-table")
  val columnsPerView: Int = config.getInt("columns-per-view")
  val defaultBaseLocation: String = config.getString("default-base-location")
  val catalogName: String = ConfigFactory.load().getString("dataset.catalog-name")
  
  /**
   * Generates all namespaces in a tree structure.
   * Returns a list of namespace paths (e.g., ["ns1", "ns1.ns2", "ns1.ns2.ns3"])
   */
  def generateNamespaces(): Seq[String] = {
    def buildTree(prefix: String, depth: Int): Seq[String] = {
      if (depth == 0) Seq.empty
      else {
        val currentLevel = (1 to namespaceWidth).map { i =>
          val name = if (prefix.isEmpty) s"ns_${depth}_$i" else s"$prefix.ns_${depth}_$i"
          name +: buildTree(name, depth - 1)
        }.flatten
        currentLevel
      }
    }
    buildTree("", namespaceDepth)
  }
  
  /**
   * Generates table identifiers for all namespaces.
   * Returns a list of (namespace, tableName) tuples.
   */
  def generateTables(): Seq[(String, String)] = {
    val namespaces = generateNamespaces()
    namespaces.flatMap { ns =>
      (1 to tablesPerNamespace).map { i =>
        (ns, s"table_$i")
      }
    }
  }
  
  /**
   * Generates view identifiers for all namespaces.
   * Returns a list of (namespace, viewName) tuples.
   */
  def generateViews(): Seq[(String, String)] = {
    val namespaces = generateNamespaces()
    namespaces.flatMap { ns =>
      (1 to viewsPerNamespace).map { i =>
        (ns, s"view_$i")
      }
    }
  }
  
  /**
   * Generates properties for a namespace.
   */
  def generateNamespaceProperties(namespace: String): Map[String, String] = {
    val seed = namespace.hashCode
    val rnd = new Random(seed)
    (1 to namespaceProperties).map { i =>
      s"prop_$i" -> s"value_${rnd.nextInt(1000)}"
    }.toMap
  }
  
  /**
   * Generates a table schema with the configured number of columns.
   */
  def generateTableSchema(tableName: String): String = {
    val seed = tableName.hashCode
    val rnd = new Random(seed)
    val types = Seq("integer", "long", "double", "string", "boolean", "date", "timestamp")
    
    val fields = (1 to columnsPerTable).map { i =>
      val fieldType = types(rnd.nextInt(types.length))
      s"""{"id": $i, "name": "col_$i", "required": false, "type": "$fieldType"}"""
    }.mkString(",\n      ")
    
    s"""{"type": "struct", "fields": [$fields]}"""
  }
  
  /**
   * Generates properties for a table.
   */
  def generateTableProperties(tableName: String): Map[String, String] = {
    val seed = tableName.hashCode
    val rnd = new Random(seed)
    (1 to tableProperties).map { i =>
      s"prop_$i" -> s"value_${rnd.nextInt(1000)}"
    }.toMap + ("write.format.default" -> "parquet")
  }
  
  /**
   * Generates a view schema with the configured number of columns.
   */
  def generateViewSchema(viewName: String): String = {
    val seed = viewName.hashCode
    val rnd = new Random(seed)
    val types = Seq("integer", "long", "double", "string", "boolean", "date", "timestamp")
    
    val fields = (1 to columnsPerView).map { i =>
      val fieldType = types(rnd.nextInt(types.length))
      s"""{"id": $i, "name": "col_$i", "required": false, "type": "$fieldType"}"""
    }.mkString(",\n      ")
    
    s"""{"type": "struct", "fields": [$fields]}"""
  }
  
  /**
   * Generates properties for a view.
   */
  def generateViewProperties(viewName: String): Map[String, String] = {
    val seed = viewName.hashCode
    val rnd = new Random(seed)
    (1 to viewProperties).map { i =>
      s"prop_$i" -> s"value_${rnd.nextInt(1000)}"
    }.toMap
  }
  
  /**
   * Converts properties map to JSON string.
   */
  def propertiesToJson(properties: Map[String, String]): String = {
    properties.map { case (k, v) => s""""$k": "$v"""" }.mkString("{", ", ", "}")
  }
  
  /**
   * Returns total number of entities that will be created.
   */
  def getTotalEntityCount: (Int, Int, Int) = {
    val namespaces = generateNamespaces()
    val tables = generateTables()
    val views = generateViews()
    (namespaces.length, tables.length, views.length)
  }
}


