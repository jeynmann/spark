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

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
 * Usage: GroupByTestSeed [numMappers] [numKVPairs] [KeySize] [numReducers]
 */
object GroupByTestSeed {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName(s"GroupBy Test Seed ${System.currentTimeMillis}")
      .getOrCreate()

    val numMappers = if (args.length > 0) args(0).toInt else 2
    val numKVPairs = if (args.length > 1) args(1).toInt else 1000
    val valSize = if (args.length > 2) args(2).toInt else 1000
    val numReducers = if (args.length > 3) args(3).toInt else numMappers
    val seed = if (args.length > 4) args(4).toLong else 100000921L
    val policy = if (args.length > 5) args(5) else "m"
    val randMax = if (args.length > 6) args(6).toInt else Int.MaxValue

    val pairs0 = spark.sparkContext.parallelize(0 until numMappers, numMappers).flatMap { p =>
      val ranGen = new Random(seed ^ p)
      val arr1 = new Array[(Int, Array[Byte])](numKVPairs)
      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        arr1(i) = (ranGen.nextInt(randMax), byteArr)
      }
      arr1
    }
    println(s"<zzh> persist=$policy, seed=$seed, next=${new Random(seed).nextLong}")
    val pairs1 = policy match {
      case "m" => println(s"<zzh> mem only")
        pairs0.cache()
      case "ms" => println(s"<zzh> mem only ser")
        pairs0.persist(StorageLevel.MEMORY_ONLY_SER)  
      case "d" => println(s"<zzh> disk only")
        pairs0.persist(StorageLevel.DISK_ONLY)  
      case "md" =>  println(s"<zzh> mem disk")
        pairs0.persist(StorageLevel.MEMORY_AND_DISK)
      case "mds" => println(s"<zzh> mem disk ser")
        pairs0.persist(StorageLevel.MEMORY_AND_DISK_SER)
      case "o" => println(s"<zzh> off heap")
        pairs0.persist(StorageLevel.OFF_HEAP)
      case _ => println(s"<zzh> mem only")
        pairs0.cache()
    }
    pairs1.count()

    println(pairs1.groupByKey(numReducers).count())

    spark.stop()
  }
}
// scalastyle:on println
