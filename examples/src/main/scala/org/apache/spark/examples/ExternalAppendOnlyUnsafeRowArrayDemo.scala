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

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Large-scale spillable collection demonstration with ~50GB shuffle data.
 * 
 * This example creates very large datasets to trigger significant spilling behavior
 * and demonstrate how Spark's spillable data structures handle massive amounts of data
 * across executor nodes.
 * 
 * Dataset sizes:
 * - Employees: ~20 million records with large text fields (~2KB each)
 * - Sales: ~50 million records with detailed transaction data (~1KB each) 
 * - Expected shuffle size: ~50GB during aggregations and joins
 * 
 * This will stress test ExternalAppendOnlyMap, ExternalSorter, and custom spillable
 * collections under realistic big data workloads.
 */
object ExternalAppendOnlyUnsafeRowArrayDemo {

  // Large data structures for realistic big data scenarios
  case class Employee(
    id: Long, 
    name: String, 
    email: String,
    departmentId: Int, 
    salary: Double, 
    age: Int,
    address: String,
    phoneNumber: String,
    skills: String,        // Large text field ~500 chars
    biography: String,     // Large text field ~1000 chars
    metadata: String       // Large text field ~500 chars
  )
  
  case class Department(
    id: Int, 
    name: String, 
    location: String,
    description: String,   // Large description ~1000 chars
    budget: Double,
    headCount: Int
  )
  
  case class Sale(
    id: Long,
    employeeId: Long, 
    customerId: Long,
    productId: Int, 
    amount: Double, 
    quantity: Int,
    month: Int,
    year: Int,
    transactionDetails: String,  // Large field ~800 chars
    customerNotes: String,       // Large field ~600 chars
    metadata: Array[Double]      // Array of 100 doubles (800 bytes)
  )

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Large-Scale Spillable Collections Demo (~50GB Shuffle)")
      .setMaster("local[8]") // Use 8 cores for better parallelism
      // Increase memory settings for large dataset processing
      .set("spark.executor.memory", "4g")
      .set("spark.executor.cores", "4")
      .set("spark.driver.memory", "2g")
      .set("spark.driver.maxResultSize", "1g")
      // Memory management for large shuffles
      .set("spark.memory.fraction", "0.8")
      .set("spark.memory.storageFraction", "0.3")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "2g")
      // Aggressive spilling settings for large data
      .set("spark.shuffle.spill.threshold", "0.6")
      .set("spark.shuffle.spill.numElementsForceSpillThreshold", "500")
      .set("spark.shuffle.spill.batchSize", "2000")
      // Optimize for large shuffles
      .set("spark.shuffle.file.buffer", "64k")
      .set("spark.shuffle.unsafe.file.output.buffer", "64k")
      .set("spark.shuffle.compress", "true")
      .set("spark.shuffle.spill.compress", "true")
      // Serialization settings for large objects
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.unsafe", "true")
      .set("spark.rdd.compress", "true")
      // Increase parallelism for large shuffles
      .set("spark.default.parallelism", "64")
      .set("spark.sql.shuffle.partitions", "64")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    try {
      println("=== Large-Scale Spillable Collections Demo (~50GB Shuffle) ===")
      println("Creating massive datasets to trigger significant spilling behavior")
      
      // Demo 1: Large-scale aggregations with ~50GB shuffle
      demonstrateMassiveAggregations(sc)
      
      // Demo 2: Large-scale joins with extensive buffering
      demonstrateMassiveJoins(sc)
      
      // Demo 3: Massive external sorting
      demonstrateMassiveSorting(sc)
      
      // Demo 4: Memory pressure with very large spillable collections
      demonstrateMassiveSpillableCollections(sc)
      
      println("\n=== Large-Scale Demo Completed - Check Spark UI for shuffle metrics ===")
      println("Expected shuffle read/write: ~50GB total across all operations")
      
    } finally {
      sc.stop()
    }
  }

  /**
   * Generate large text content for realistic big data scenarios
   */
  def generateLargeText(baseLength: Int, seed: Int): String = {
    val random = new Random(seed)
    val words = Array("data", "processing", "analytics", "machine", "learning", "artificial", 
                     "intelligence", "distributed", "computing", "scalable", "performance", 
                     "optimization", "algorithm", "framework", "cluster", "memory", "storage",
                     "execution", "partition", "shuffle", "serialization", "compression")
    
    (1 to baseLength / 8).map { _ =>
      words(random.nextInt(words.length))
    }.mkString(" ") + s" [Generated content with seed $seed for realistic data size]"
  }

  /**
   * Massive aggregations to generate ~15GB of shuffle data
   */
  def demonstrateMassiveAggregations(sc: SparkContext): Unit = {
    println("\n1. Massive Aggregations Demo (~15GB shuffle):")
    
    // Create 20 million employee records (~40GB raw data)
    val massiveEmployeeRDD = sc.parallelize(1L to 20000000L, numSlices = 128).mapPartitions { partition =>
      val partitionId = java.util.UUID.randomUUID().toString.take(8)
      println(s"[EXECUTOR-$partitionId] Generating massive employee dataset")
      
      partition.map { i =>
        val random = new Random(i) // Deterministic but varied
        Employee(
          id = i,
          name = s"Employee_${i}_${random.nextInt(10000)}",
          email = s"emp${i}@company${random.nextInt(1000)}.com",
          departmentId = (i % 2000 + 1).toInt, // 2000 departments for high cardinality
          salary = 30000 + random.nextDouble() * 150000,
          age = 22 + random.nextInt(43),
          address = generateLargeText(200, i.toInt),           // ~200 chars
          phoneNumber = f"555-${random.nextInt(10000)}%04d-${random.nextInt(10000)}%04d",
          skills = generateLargeText(500, i.toInt + 1000),     // ~500 chars
          biography = generateLargeText(1000, i.toInt + 2000), // ~1000 chars  
          metadata = generateLargeText(500, i.toInt + 3000)    // ~500 chars
        )
      }
    }
    
    println("   Generated 20M employee records (~40GB raw data)")
    
    // Massive groupByKey aggregation - will cause significant shuffle
    val departmentAggregations = massiveEmployeeRDD
      .map(emp => (emp.departmentId, emp))
      .mapPartitions { partition =>
        val partitionId = java.util.UUID.randomUUID().toString.take(8)
        var count = 0
        partition.map { item =>
          count += 1
          if (count % 50000 == 0) {
            println(s"[EXECUTOR-$partitionId] Processed $count records for groupByKey")
          }
          item
        }
      }
      .groupByKey() // Major shuffle operation - ~15GB
      .mapPartitions { partition =>
        val partitionId = java.util.UUID.randomUUID().toString.take(8)
        println(s"[EXECUTOR-$partitionId] Processing grouped employee data")
        
        partition.map { case (deptId, employees) =>
          val empList = employees.toList
          val totalSalary = empList.map(_.salary).sum
          val avgSalary = totalSalary / empList.size
          val avgAge = empList.map(_.age).sum.toDouble / empList.size
          val empCount = empList.size
          
          // Calculate text length statistics 
          val avgBioLength = empList.map(_.biography.length).sum.toDouble / empList.size
          val avgSkillsLength = empList.map(_.skills.length).sum.toDouble / empList.size
          
          if (deptId <= 10) {
            println(f"[EXECUTOR-$partitionId] Dept $deptId: $empCount employees, " +
                   f"avg salary = $$${avgSalary}%.2f, avg bio length = ${avgBioLength}%.0f")
          }
          
          (deptId, avgSalary, avgAge, empCount, avgBioLength, avgSkillsLength)
        }
      }
    
    val aggregationCount = departmentAggregations.count()
    println(f"   Completed massive aggregation: $aggregationCount departments")
    println("   *** Check Spark UI - Shuffle Read/Write should show ~15GB ***")
  }

  /**
   * Massive joins with extensive data buffering (~20GB shuffle)
   */
  def demonstrateMassiveJoins(sc: SparkContext): Unit = {
    println("\n2. Massive Join Operations Demo (~20GB shuffle):")
    
    // Create 50 million sales records (~50GB raw data)  
    val massiveSalesRDD = sc.parallelize(1L to 50000000L, numSlices = 256).mapPartitions { partition =>
      val partitionId = java.util.UUID.randomUUID().toString.take(8)
      println(s"[EXECUTOR-$partitionId] Generating massive sales dataset")
      
      partition.map { i =>
        val random = new Random(i)
        Sale(
          id = i,
          employeeId = i % 20000000L + 1L, // Reference 20M employees  
          customerId = i % 5000000L + 1L,  // 5M customers
          productId = (i % 10000 + 1).toInt, // 10K products
          amount = random.nextDouble() * 10000,
          quantity = random.nextInt(100) + 1,
          month = (i % 12 + 1).toInt,
          year = 2020 + (i % 5).toInt,
          transactionDetails = generateLargeText(800, i.toInt),      // ~800 chars
          customerNotes = generateLargeText(600, i.toInt + 10000),   // ~600 chars
          metadata = Array.fill(100)(random.nextDouble())            // 100 doubles = 800 bytes
        )
      }
    }
    
    println("   Generated 50M sales records (~50GB raw data)")
    
    // Create employee lookup data (subset for join)
    val employeeLookupRDD = sc.parallelize(1L to 20000000L, numSlices = 64).mapPartitions { partition =>
      val partitionId = java.util.UUID.randomUUID().toString.take(8)
      println(s"[EXECUTOR-$partitionId] Creating employee lookup data")
      
      partition.map { empId =>
        val random = new Random(empId)
        val deptId = (empId % 2000 + 1).toInt
        (empId, (s"Employee_$empId", deptId, generateLargeText(300, empId.toInt)))
      }
    }
    
    // Massive cogroup operation - simulates join with buffering
    val salesByEmployee = massiveSalesRDD.map(sale => (sale.employeeId, sale))
    
    val massiveJoinRDD = salesByEmployee.cogroup(employeeLookupRDD).mapPartitions { partition =>
      val partitionId = java.util.UUID.randomUUID().toString.take(8)
      println(s"[EXECUTOR-$partitionId] Processing massive cogroup (join-like operation)")
      
      var processedPairs = 0
      partition.flatMap { case (empId, (sales, employees)) =>
        val salesList = sales.toList
        val empList = employees.toList
        
        processedPairs += 1
        if (processedPairs % 10000 == 0) {
          println(s"[EXECUTOR-$partitionId] Processed $processedPairs employee groups")
        }
        
        if (salesList.nonEmpty && empList.nonEmpty) {
          val totalSales = salesList.map(_.amount).sum
          val avgSale = totalSales / salesList.size
          val (empName, deptId, empData) = empList.head
          
          Some((deptId, empId, empName, salesList.size, totalSales, avgSale, empData.length))
        } else {
          None
        }
      }
    }
    
    val joinCount = massiveJoinRDD.count()
    println(f"   Completed massive join: $joinCount employee-sales pairs")
    println("   *** Check Spark UI - Additional ~20GB shuffle for cogroup operation ***")
    
    // Additional aggregation on join results
    val joinAggregation = massiveJoinRDD
      .map { case (deptId, empId, empName, salesCount, totalSales, avgSale, empDataLen) =>
        (deptId, (totalSales, salesCount, empDataLen))
      }
      .reduceByKey { case ((sales1, count1, len1), (sales2, count2, len2)) =>
        (sales1 + sales2, count1 + count2, len1 + len2)
      }
    
    val deptStats = joinAggregation.collect()
    println(f"   Department aggregation from join: ${deptStats.length} departments")
  }

  /**
   * Massive external sorting (~10GB shuffle)
   */
  def demonstrateMassiveSorting(sc: SparkContext): Unit = {
    println("\n3. Massive External Sorting Demo (~10GB shuffle):")
    
    // Create large dataset specifically designed for sorting
    val massiveSortDataRDD = sc.parallelize(1L to 30000000L, numSlices = 128).mapPartitions { partition =>
      val partitionId = java.util.UUID.randomUUID().toString.take(8)
      println(s"[EXECUTOR-$partitionId] Creating massive sort dataset")
      
      partition.map { i =>
        val random = new Random(i)
        val sortKey = random.nextInt(5000000) // 5M unique sort keys for realistic distribution
        val largePayload = generateLargeText(1000, i.toInt) // 1KB payload per record
        val numericData = Array.fill(50)(random.nextDouble()) // 400 bytes of numeric data
        
        (sortKey, largePayload, numericData, i)
      }
    }
    
    println("   Generated 30M records for sorting (~30GB raw data)")
    
    // Massive sortByKey operation
    val massiveSortedRDD = massiveSortDataRDD
      .map { case (key, payload, numData, id) => (key, (payload, numData, id)) }
      .mapPartitions { partition =>
        val partitionId = java.util.UUID.randomUUID().toString.take(8)
        var count = 0
        partition.map { item =>
          count += 1
          if (count % 100000 == 0) {
            println(s"[EXECUTOR-$partitionId] Prepared $count records for sorting")
          }
          item
        }
      }
      .sortByKey() // Major sort operation with external spilling
      .mapPartitions { partition =>
        val partitionId = java.util.UUID.randomUUID().toString.take(8)
        println(s"[EXECUTOR-$partitionId] Verifying sorted order")
        
        var lastKey = Int.MinValue
        var sortedCount = 0
        
        partition.map { case (key, (payload, numData, id)) =>
          assert(key >= lastKey, s"Sort violation: $key < $lastKey")
          lastKey = key
          sortedCount += 1
          
          if (sortedCount <= 5) {
            println(s"[EXECUTOR-$partitionId] Sorted record $sortedCount: key=$key, payload_len=${payload.length}")
          }
          
          (key, payload.length, numData.sum, id)
        }
      }
    
    val sortedCount = massiveSortedRDD.count()
    println(f"   Completed massive sort: $sortedCount records")
    println("   *** Check Spark UI - External sorting should show ~10GB shuffle ***")
  }

  /**
   * Massive spillable collections with extreme memory pressure
   */
  def demonstrateMassiveSpillableCollections(sc: SparkContext): Unit = {
    println("\n4. Massive Spillable Collections Demo (~5GB additional processing):")
    
    // Large spillable array for executor-side processing
    class MassiveSpillableArray[T](threshold: Int) extends Serializable {
      private val buffer = ArrayBuffer[T]()
      private var spillCount = 0
      private var totalSpilledItems = 0L
      private val partitionId = java.util.UUID.randomUUID().toString.take(8)
      
      def add(item: T): Unit = {
        buffer += item
        if (buffer.size >= threshold) {
          spill()
        }
      }
      
      private def spill(): Unit = {
        spillCount += 1
        totalSpilledItems += buffer.size
        println(s"[EXECUTOR-$partitionId] SPILL #$spillCount: ${buffer.size} items " +
               f"(total spilled: $totalSpilledItems)")
        // Simulate disk write time
        Thread.sleep(10)
        buffer.clear()
      }
      
      def finalizeAndGetStats(): (Int, Int, Long) = {
        val finalSize = buffer.size
        if (finalSize > 0) {
          println(s"[EXECUTOR-$partitionId] Final: size=$finalSize, spills=$spillCount, " +
                 f"total_spilled=$totalSpilledItems")
        }
        (finalSize, spillCount, totalSpilledItems)
      }
    }
    
    // Process large dataset with custom spillable collections
    val massiveSpillableRDD = sc.parallelize(1L to 10000000L, numSlices = 32).mapPartitions { partition =>
      val partitionId = java.util.UUID.randomUUID().toString.take(8)
      println(s"[EXECUTOR-$partitionId] Starting massive spillable array processing")
      
      val spillableArray = new MassiveSpillableArray[String](200) // Very low threshold for frequent spilling
      var processedCount = 0L
      
      partition.foreach { i =>
        val largeItem = generateLargeText(2000, i.toInt) // 2KB per item
        spillableArray.add(largeItem)
        processedCount += 1
        
        if (processedCount % 25000 == 0) {
          println(s"[EXECUTOR-$partitionId] Processed $processedCount items (spillable array)")
        }
      }
      
      val (finalSize, spillCount, totalSpilled) = spillableArray.finalizeAndGetStats()
      Iterator((processedCount, finalSize, spillCount, totalSpilled))
    }
    
    // Collect spillable array statistics
    val spillableStats = massiveSpillableRDD.reduce { 
      case ((p1, f1, s1, t1), (p2, f2, s2, t2)) =>
        (p1 + p2, f1 + f2, s1 + s2, t1 + t2)
    }
    
    val (totalProcessed, totalFinalSize, totalSpills, totalSpilledItems) = spillableStats
    println(f"\n   Massive Spillable Collections Results:")
    println(f"     Total items processed: $totalProcessed")
    println(f"     Total final buffer size: $totalFinalSize")
    println(f"     Total spill operations: $totalSpills")
    println(f"     Total items spilled to disk: $totalSpilledItems")
    println(f"     Spill ratio: ${totalSpilledItems.toDouble / totalProcessed * 100}%.1f%%")
  }
}
// scalastyle:on println
