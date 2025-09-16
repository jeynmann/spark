/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples

import scala.util.Random

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Demonstrates Spark's BytesToBytesMap usage through hash-based aggregations and joins.
 * 
 * BytesToBytesMap is used internally by Spark for:
 * 1. Hash-based aggregations (GROUP BY with fixed-width data types)
 * 2. Broadcast hash joins 
 * 3. External sorting operations
 *
 * This example generates random tables and performs operations that trigger BytesToBytesMap usage.
 */
object BytesToBytesMapDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("BytesToBytesMap Demo")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      // Force hash-based aggregations (avoid sort-based fallback)
      .config("spark.sql.execution.useObjectHashAggregateExec", "false")
      // Enable off-heap memory for BytesToBytesMap
      .config("spark.memory.offHeap.enabled", "true")
      .config("spark.memory.offHeap.size", "512m")
      // Optimize for hash operations
      .config("spark.buffer.pageSize", "64m")
      .config("spark.sql.execution.arrow.pyspark.enabled", "false")
      .getOrCreate()

    try {
      println("=== BytesToBytesMap Demo Started ===")
      
      // Generate random tables with fixed-width types (triggers BytesToBytesMap)
      val salesTable = generateSalesTable(spark, numRecords = 100000)
      val departmentTable = generateDepartmentTable(spark, numDepartments = 50)
      val employeeTable = generateEmployeeTable(spark, numEmployees = 10000)

      println("\n1. Generated Random Tables with Fixed-Width Data Types:")
      println("   - Sales Table: 100K records (int, long, double types)")
      println("   - Department Table: 50 records (for broadcast join)")
      println("   - Employee Table: 10K records")

      // Cache tables for reuse
      salesTable.cache()
      departmentTable.cache()
      employeeTable.cache()

      // Trigger BytesToBytesMap through hash-based aggregations
      demonstrateHashAggregations(salesTable)

      // Trigger BytesToBytesMap through broadcast hash joins
      demonstrateBroadcastHashJoins(spark, salesTable, departmentTable, employeeTable)

      // Demonstrate complex aggregations that use BytesToBytesMap
      demonstrateComplexAggregations(salesTable, employeeTable)

      // Show execution plans to verify BytesToBytesMap usage
      demonstrateExecutionPlans(spark, salesTable, departmentTable)

      println("\n=== BytesToBytesMap Demo Completed ===")
      println("Check the Spark UI and execution plans above to see BytesToBytesMap usage in:")
      println("- HashAggregateExec operators (uses UnsafeFixedWidthAggregationMap)")
      println("- BroadcastHashJoinExec operators (uses HashedRelation)")
      println("- External sorting operations when memory pressure occurs")

    } finally {
      spark.stop()
    }
  }

  /**
   * Generate a sales table with fixed-width data types (triggers BytesToBytesMap in aggregations)
   */
  def generateSalesTable(spark: SparkSession, numRecords: Int): DataFrame = {
    import spark.implicits._
    
    val random = new Random(42) // Fixed seed for reproducibility
    val salesData = (1 to numRecords).map { _ =>
      (
        random.nextInt(50) + 1,           // department_id (1-50)
        random.nextInt(10000) + 1,        // employee_id (1-10000) 
        random.nextInt(5) + 1,            // product_id (1-5)
        random.nextDouble() * 1000 + 10,  // amount (10-1010)
        random.nextInt(12) + 1,           // month (1-12)
        2024,                             // year
        System.currentTimeMillis() + random.nextInt(1000000) // timestamp
      )
    }
    
    salesData.toDF("department_id", "employee_id", "product_id", "amount", "month", "year", "timestamp")
  }

  /**
   * Generate a small department table (good for broadcast joins)
   */
  def generateDepartmentTable(spark: SparkSession, numDepartments: Int): DataFrame = {
    import spark.implicits._
    
    val departments = List("Engineering", "Sales", "Marketing", "HR", "Finance", 
                          "Operations", "Support", "Research", "Legal", "Admin")
    
    val deptData = (1 to numDepartments).map { id =>
      (
        id,
        departments(id % departments.length) + s"_$id",
        (id % 5) + 1, // region_id (1-5)
        50 + (id * 10) % 200 // budget_multiplier
      )
    }
    
    deptData.toDF("department_id", "department_name", "region_id", "budget_multiplier")
  }

  /**
   * Generate employee table with fixed-width types
   */
  def generateEmployeeTable(spark: SparkSession, numEmployees: Int): DataFrame = {
    import spark.implicits._
    
    val random = new Random(123)
    val employeeData = (1 to numEmployees).map { id =>
      (
        id,
        random.nextInt(50) + 1,        // department_id
        random.nextInt(5) + 1,         // level (1-5)
        30000 + random.nextInt(120000), // salary (30k-150k)
        random.nextInt(15),            // years_experience (0-14)
        if (random.nextBoolean()) 1 else 0 // is_manager (0 or 1)
      )
    }
    
    employeeData.toDF("employee_id", "department_id", "level", "salary", "years_experience", "is_manager")
  }

  /**
   * Demonstrate hash-based aggregations that use BytesToBytesMap
   */
  def demonstrateHashAggregations(salesTable: DataFrame): Unit = {
    println("\n2. Hash-Based Aggregations (using BytesToBytesMap internally):")
    
    // Simple GROUP BY aggregation - uses UnsafeFixedWidthAggregationMap with BytesToBytesMap
    println("   a) Sales by Department:")
    val salesByDept = salesTable
      .groupBy("department_id")
      .agg(
        count("*").alias("total_sales"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount"),
        max("amount").alias("max_amount"),
        min("amount").alias("min_amount")
      )
      .orderBy("total_amount")
    
    salesByDept.show(10)

    // Multi-level GROUP BY aggregation
    println("   b) Sales by Department and Month:")
    val salesByDeptMonth = salesTable
      .groupBy("department_id", "month")
      .agg(
        count("*").alias("sales_count"),
        sum("amount").alias("monthly_total")
      )
      .filter(col("monthly_total") > 5000)
      .orderBy("department_id", "month")
    
    salesByDeptMonth.show(20)

    // Complex aggregation with multiple grouping keys
    println("   c) Sales Summary by Product and Department:")
    salesTable
      .groupBy("product_id", "department_id")
      .agg(
        count("*").alias("transaction_count"),
        sum("amount").alias("product_dept_total"),
        countDistinct("employee_id").alias("unique_employees")
      )
      .orderBy(desc("product_dept_total"))
      .show(15)
  }

  /**
   * Demonstrate broadcast hash joins that use BytesToBytesMap
   */
  def demonstrateBroadcastHashJoins(spark: SparkSession, salesTable: DataFrame, 
                                   departmentTable: DataFrame, employeeTable: DataFrame): Unit = {
    println("\n3. Broadcast Hash Joins (using BytesToBytesMap internally):")
    
    // Force broadcast join of small department table
    println("   a) Sales with Department Info (Broadcast Hash Join):")
    val salesWithDepts = salesTable
      .join(broadcast(departmentTable), "department_id")
      .select("department_name", "amount", "month", "employee_id")
    
    salesWithDepts.show(10)

    // Broadcast join with aggregation
    println("   b) Department Revenue Summary:")
    val deptRevenue = salesTable
      .join(broadcast(departmentTable), "department_id")
      .groupBy("department_name", "region_id")
      .agg(
        sum("amount").alias("total_revenue"),
        count("*").alias("total_transactions"),
        avg("amount").alias("avg_transaction")
      )
      .orderBy(desc("total_revenue"))
    
    deptRevenue.show(10)

    // Multi-table broadcast join
    println("   c) Employee Performance with Department Context:")
    val empPerformance = salesTable
      .join(broadcast(employeeTable), "employee_id")
      .join(broadcast(departmentTable), "department_id")
      .groupBy("department_name", "level", "is_manager")
      .agg(
        sum("amount").alias("sales_total"),
        count("*").alias("sales_count"),
        avg("salary").alias("avg_salary")
      )
      .filter(col("sales_count") > 5)
      .orderBy("department_name", "level")
    
    empPerformance.show(20)
  }

  /**
   * Demonstrate complex aggregations that stress BytesToBytesMap
   */
  def demonstrateComplexAggregations(salesTable: DataFrame, employeeTable: DataFrame): Unit = {
    println("\n4. Complex Aggregations (stressing BytesToBytesMap):")
    
    // High-cardinality GROUP BY to test BytesToBytesMap limits
    println("   a) High-Cardinality Aggregation:")
    val highCardAgg = salesTable
      .groupBy("employee_id", "product_id", "month")
      .agg(
        sum("amount").alias("emp_product_monthly_total"),
        count("*").alias("transaction_count")
      )
      .filter(col("transaction_count") > 2)
      .orderBy(desc("emp_product_monthly_total"))
    
    println(s"   High-cardinality result count: ${highCardAgg.count()}")
    highCardAgg.show(10)

    // Simple time-based aggregation instead of window function
    println("   b) Time-Based Aggregation:")
    val timeBasedStats = salesTable
      .withColumn("hour", hour(col("timestamp").cast("timestamp")))
      .groupBy("department_id", "hour")
      .agg(
        sum("amount").alias("hourly_total"),
        count("*").alias("hourly_count")
      )
      .withColumn("rank", row_number().over(
        org.apache.spark.sql.expressions.Window
          .partitionBy("department_id")
          .orderBy(desc("hourly_total"))
      ))
      .filter(col("rank") <= 5)
      .orderBy("department_id", "rank")
    
    timeBasedStats.show(20)
  }

  /**
   * Show execution plans to verify BytesToBytesMap usage
   */
  def demonstrateExecutionPlans(spark: SparkSession, salesTable: DataFrame, departmentTable: DataFrame): Unit = {
    println("\n5. Execution Plans (showing BytesToBytesMap usage):")
    
    // Plan for hash aggregation (uses UnsafeFixedWidthAggregationMap)
    println("   a) Hash Aggregation Plan:")
    val aggPlan = salesTable
      .groupBy("department_id")
      .agg(sum("amount").alias("total"))
    
    aggPlan.explain(true)
    
    // Plan for broadcast hash join (uses HashedRelation)
    println("\n   b) Broadcast Hash Join Plan:")
    val joinPlan = salesTable
      .join(broadcast(departmentTable), "department_id")
    
    joinPlan.explain(true)
    
    // Execute the plans to trigger actual BytesToBytesMap usage
    println(s"\n   Aggregation result count: ${aggPlan.count()}")
    println(s"   Join result count: ${joinPlan.count()}")
  }
}
// scalastyle:on println
