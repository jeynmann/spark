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

import java.util.Random

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Demonstrates UnsafeExternalSorter usage through various sorting operations.
 * This application generates large random datasets and performs sorting operations
 * that trigger external sorting when memory limits are exceeded.
 * 
 * UnsafeExternalSorter is used internally by Spark for:
 * - Large shuffle operations that exceed memory
 * - Sort-based aggregations
 * - Window operations with large frames
 * - Custom sorting with complex keys
 * 
 * The demo is optimized for:
 * - Multiple executors with single cores each
 * - Aggressive memory pressure to force external sorting
 * - Large shuffle sizes (targeting ~50GB total)
 * 
 * Usage:
 * bin/spark-submit run-example \
 *   --num-executors 10 \
 *   --executor-cores 1 \
 *   --executor-memory 4g \
 *   --driver-memory 2g \
 *   UnsafeExternalSorterDemo [recordsPerPartition] [numPartitions]
 * 
 * Arguments:
 *   recordsPerPartition: Number of records per partition (default: 50000)
 *   numPartitions: Number of partitions (default: 30)
 */
object UnsafeExternalSorterDemo {

  // Data structures optimized for single-core sorting
  case class SortableRecord(
    primaryKey: Long,
    secondaryKey: Int, 
    category: String,
    value: Double,
    data: String,           // Large text field to increase memory pressure
    metadata: Array[Byte]   // Binary data to add size
  )
  
  case class CompositeKey(
    department: Int,
    timestamp: Long,
    priority: Int
  ) extends Ordered[CompositeKey] {
    def compare(that: CompositeKey): Int = {
      val deptCompare = this.department.compare(that.department)
      if (deptCompare != 0) deptCompare
      else {
        val timestampCompare = this.timestamp.compare(that.timestamp)
        if (timestampCompare != 0) timestampCompare
        else this.priority.compare(that.priority)
      }
    }
  }
  
  case class LargePayload(
    id: Long,
    content: String,        // ~2KB content
    numbers: Array[Double], // 200 doubles = 1.6KB
    flags: Array[Boolean]   // 100 booleans
  )

  def main(args: Array[String]): Unit = {
    // Parse command line arguments
    val recordsPerPartition = if (args.length > 0) args(0).toInt else 50000
    val numPartitions = if (args.length > 1) args(1).toInt else 30
    
    val conf = new SparkConf()
      .setAppName("UnsafeExternalSorter Demo - External Sorting Operations")
      
      // Memory management optimized for external sorting
      // Note: Don't set master, executors, or deployment - let run-example handle those
      .set("spark.memory.fraction", "0.8")
      .set("spark.memory.storageFraction", "0.2")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1g")
      
      // Aggressive spilling settings to force UnsafeExternalSorter usage
      .set("spark.shuffle.sort.bypassMergeThreshold", "5")   // Force sorting for small partitions
      .set("spark.shuffle.spill.threshold", "0.4")           // Aggressive spilling at 40%
      .set("spark.shuffle.spill.numElementsForceSpillThreshold", "50") // Force spill after 50 elements
      .set("spark.shuffle.spill.batchSize", "500")
      
      // Shuffle optimization
    //   .set("spark.shuffle.file.buffer", "32k")
    //   .set("spark.shuffle.unsafe.file.output.buffer", "32k")
    //   .set("spark.shuffle.compress", "true")
    //   .set("spark.shuffle.spill.compress", "true")
    //   .set("spark.shuffle.service.enabled", "true")
      
      // Serialization settings for performance
    //   .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //   .set("spark.kryo.unsafe", "true")
    //   .set("spark.kryo.referenceTracking", "false")
      
      // Parallelism settings (will be adjusted based on actual cluster size)
      .set("spark.sql.shuffle.partitions", numPartitions.toString)

    val sc = new SparkContext(conf)
    sc.setLogLevel("INFO") // Show spilling information

    try {
      println("=== UnsafeExternalSorter Demo - External Sorting Operations ===")
      println("Designed to trigger external sorting through memory pressure")
      println(s"Spark Application ID: ${sc.applicationId}")
      println(s"Records per partition: $recordsPerPartition")
      println(s"Number of partitions: $numPartitions")
      println(s"Total records: ${recordsPerPartition * numPartitions}")
      println("Watch for 'Spilling' messages in logs indicating UnsafeExternalSorter usage")
      
      // Demo 1: Large-scale shuffle sorting
      demonstrateLargeShuffleSorting(sc, recordsPerPartition, numPartitions)
      
      // Demo 2: Complex key sorting with external sorting
      demonstrateComplexKeySorting(sc, recordsPerPartition, numPartitions)
      
      // Demo 3: Memory pressure sorting operations
      demonstrateMemoryPressureSorting(sc, recordsPerPartition, numPartitions)
      
      // Demo 4: Secondary sorting patterns
      demonstrateSecondarySorting(sc, recordsPerPartition, numPartitions)
      
      // Demo 5: Custom partitioner with sorting
      demonstrateCustomPartitionerSorting(sc, recordsPerPartition, numPartitions)
      
      println("\n=== UnsafeExternalSorter Demo Completed ===")
      println("Check executor logs for 'Spilling' messages from UnsafeExternalSorter")
      
    } finally {
      sc.stop()
    }
  }

  /**
   * Generate large random content
   */
  def generateLargeContent(baseSize: Int, seed: Int): String = {
    val random = new Random(seed)
    val chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "
    (1 to baseSize).map(_ => chars(random.nextInt(chars.length))).mkString
  }

  /**
   * Large-scale shuffle sorting with external sorting
   */
  def demonstrateLargeShuffleSorting(sc: SparkContext, recordsPerPartition: Int, numPartitions: Int): Unit = {
    println("\n1. Large-Scale Shuffle Sorting Demo (UnsafeExternalSorter):")
    
    // Generate large dataset with shuffle
    val largeRDD = sc.parallelize(1 to numPartitions, numPartitions).flatMap { partitionId =>
      val random = new Random(partitionId)
      (1 to recordsPerPartition).map { i =>
        val key = random.nextLong() % 1000000  // Create sorting pressure
        val data = generateLargeContent(2048, random.nextInt()) // 2KB per record
        val metadata = new Array[Byte](1024) // 1KB metadata
        random.nextBytes(metadata)
        
        SortableRecord(
          primaryKey = key,
          secondaryKey = random.nextInt(10000),
          category = s"category_${random.nextInt(100)}",
          value = random.nextDouble() * 10000,
          data = data,
          metadata = metadata
        )
      }
    }
    
    println(s"Generated ${largeRDD.count()} large records")
    
    // Force external sorting through shuffle
    val sortedByKey = largeRDD.sortBy(_.primaryKey, ascending = true)
    println(f"Sorted by primary key: ${sortedByKey.count()} records")
    println(f"First 3 sorted keys: ${sortedByKey.map(_.primaryKey).take(3).mkString(", ")}")
    
    // Multi-column sort (triggers more complex sorting)
    val complexSort = largeRDD.sortBy(r => (r.category, -r.value, r.secondaryKey))
    println(f"Complex multi-column sort completed: ${complexSort.count()} records")
    
    largeRDD.unpersist()
  }

  /**
   * Complex key sorting that forces external sorting
   */
  def demonstrateComplexKeySorting(sc: SparkContext, recordsPerPartition: Int, numPartitions: Int): Unit = {
    println("\n2. Complex Key Sorting Demo (UnsafeExternalSorter):")
    
    // Generate data with composite keys
    val compositeKeyRDD = sc.parallelize(1 to numPartitions, numPartitions).flatMap { partitionId =>
      val random = new Random(partitionId * 1000)
      (1 to recordsPerPartition).map { i =>
        val key = CompositeKey(
          department = random.nextInt(50),
          timestamp = System.currentTimeMillis() - random.nextInt(86400000), // Last 24h
          priority = random.nextInt(10)
        )
        
        val content = generateLargeContent(1500, random.nextInt()) // 1.5KB
        val numbers = Array.fill(200)(random.nextDouble())         // 1.6KB
        val flags = Array.fill(100)(random.nextBoolean())          // 100 bytes
        
        (key, LargePayload(i.toLong, content, numbers, flags))
      }
    }
    
    println(f"Generated ${compositeKeyRDD.count()} records with composite keys")
    
    // Sort by composite key (triggers UnsafeExternalSorter for complex comparisons)
    val sortedByComposite = compositeKeyRDD.sortByKey(ascending = true)
    println(f"Sorted by composite key: ${sortedByComposite.count()} records")
    
    // Show first few sorted keys
    val firstKeys = sortedByComposite.map(_._1).take(3)
    println("First 3 composite keys after sorting:")
    firstKeys.foreach(k => println(s"  Dept: ${k.department}, Time: ${k.timestamp}, Priority: ${k.priority}"))
    
    compositeKeyRDD.unpersist()
  }

  /**
   * Memory pressure sorting to force external sorting
   */
  def demonstrateMemoryPressureSorting(sc: SparkContext, recordsPerPartition: Int, numPartitions: Int): Unit = {
    println("\n3. Memory Pressure Sorting Demo (UnsafeExternalSorter):")
    
    // Create even larger records to increase memory pressure
    val memoryIntensiveRDD = sc.parallelize(1 to numPartitions, numPartitions).flatMap { partitionId =>
      val random = new Random(partitionId * 2000)
      (1 to recordsPerPartition / 2).map { i => // Fewer but larger records
        val key = random.nextLong()
        val largeData = generateLargeContent(8192, random.nextInt()) // 8KB per record
        val largeMetadata = new Array[Byte](4096) // 4KB metadata
        random.nextBytes(largeMetadata)
        
        (key, (largeData, largeMetadata, random.nextDouble() * 100000))
      }
    }
    
    println(f"Generated ${memoryIntensiveRDD.count()} memory-intensive records")
    
    // Multiple sorting operations to increase pressure
    val sorted1 = memoryIntensiveRDD.sortByKey(ascending = true)
    println(f"First sort completed: ${sorted1.count()} records")
    
    val sorted2 = memoryIntensiveRDD.sortBy(_._2._3, ascending = false) // Sort by double value
    println(f"Second sort completed: ${sorted2.count()} records")
    
    // Repartition and sort again (forces shuffle + sort)
    val reshuffled = memoryIntensiveRDD.repartition(numPartitions * 2).sortByKey()
    println(f"Reshuffled and sorted: ${reshuffled.count()} records")
    
    memoryIntensiveRDD.unpersist()
  }

  /**
   * Secondary sorting pattern demonstration
   */
  def demonstrateSecondarySorting(sc: SparkContext, recordsPerPartition: Int, numPartitions: Int): Unit = {
    println("\n4. Secondary Sorting Demo (UnsafeExternalSorter):")
    
    // Generate data for secondary sorting
    val secondarySortRDD = sc.parallelize(1 to numPartitions, numPartitions).flatMap { partitionId =>
      val random = new Random(partitionId * 3000)
      (1 to recordsPerPartition).map { i =>
        val groupKey = s"group_${random.nextInt(100)}" // 100 groups
        val sortKey = random.nextDouble()
        val data = generateLargeContent(1024, random.nextInt()) // 1KB
        
        ((groupKey, sortKey), data)
      }
    }
    
    println(f"Generated ${secondarySortRDD.count()} records for secondary sorting")
    
    // Perform secondary sorting: sort by group first, then by sort key within group
    val secondarySorted = secondarySortRDD
      .repartitionAndSortWithinPartitions(new org.apache.spark.HashPartitioner(numPartitions))
    
    println(f"Secondary sort completed: ${secondarySorted.count()} records")
    
    // Group and sort within each group
    val groupedAndSorted = secondarySortRDD
      .groupByKey()
      .mapValues(values => values.toList.sorted)
    
    println(f"Grouped and sorted within groups: ${groupedAndSorted.count()} groups")
    
    secondarySortRDD.unpersist()
  }

  /**
   * Custom partitioner with sorting
   */
  def demonstrateCustomPartitionerSorting(sc: SparkContext, recordsPerPartition: Int, numPartitions: Int): Unit = {
    println("\n5. Custom Partitioner Sorting Demo (UnsafeExternalSorter):")
    
    // Custom partitioner that forces uneven distribution
    class CustomPartitioner(partitions: Int) extends org.apache.spark.Partitioner {
      override def numPartitions: Int = partitions
      
      override def getPartition(key: Any): Int = {
        key match {
          case s: String => 
            // Intentionally create skewed partitioning to increase sorting pressure
            val hash = s.hashCode
            Math.abs(hash % 3) // Force most data into first 3 partitions
          case _ => 0
        }
      }
    }
    
    // Generate data with string keys
    val customPartitionRDD = sc.parallelize(1 to numPartitions, numPartitions).flatMap { partitionId =>
      val random = new Random(partitionId * 4000)
      (1 to recordsPerPartition).map { i =>
        val key = f"key_${random.nextInt(1000)}%04d"
        val payload = generateLargeContent(2048, random.nextInt())
        (key, payload)
      }
    }
    
    println(f"Generated ${customPartitionRDD.count()} records for custom partitioning")
    
    // Apply custom partitioner and sort
    val customPartitioned = customPartitionRDD
      .partitionBy(new CustomPartitioner(numPartitions))
      .sortByKey()
    
    println(f"Custom partitioned and sorted: ${customPartitioned.count()} records")
    
    // Check partition distribution
    val partitionSizes = customPartitioned.mapPartitionsWithIndex { (idx, iter) =>
      Iterator((idx, iter.size))
    }.collect()
    
    println("Partition distribution:")
    partitionSizes.take(10).foreach { case (idx, size) =>
      println(f"  Partition $idx: $size records")
    }
    
    customPartitionRDD.unpersist()
  }
}
// scalastyle:on println