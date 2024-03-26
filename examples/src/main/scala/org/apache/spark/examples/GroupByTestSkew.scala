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
object GroupByTestSkew {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName(s"GroupBy Test Skew ${System.currentTimeMillis}")
      .getOrCreate()

    val numMappers = if (args.length > 0) args(0).toInt else 2
    val numKVPairs = if (args.length > 1) args(1).toInt else 1000
    val valSize = if (args.length > 2) args(2).toInt else 1000
    val numReducers = if (args.length > 3) args(3).toInt else numMappers
    val kSkew = if (args.length > 4) args(4).toInt.min(numKVPairs) else numKVPairs
    val numSkew = if (args.length > 5) args(5).toInt else 2
    val policy = if (args.length > 6) args(6) else "m"
    val seed = if (args.length > 7) args(7).toLong else 100000921L
    val randMax = if (args.length > 8) args(8).toInt else Int.MaxValue

    // numMappers * numKVPairs
    val pairs0 = spark.sparkContext.parallelize(0 until numMappers, numMappers).flatMap { p =>
      val ranGen = new Random(seed)
      val arr1 = new Array[(Int, Array[Byte])](numKVPairs)
      val range = Array(0, kSkew)
      while (range(1) <= numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        for (i <- range(0) until range(1).min(numKVPairs)) {
            arr1(i) = (range(0) + p / numSkew, byteArr)
        }
        val d = range(1) - range(0)
        range(0) = range(1)
        range(1) = (range(1) + kSkew)
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
