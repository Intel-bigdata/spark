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

package org.apache.spark.examples

import org.apache.spark.sql.SparkSession

object WordCountExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Word Count Example")
      .config("spark.shuffle.plasma.enabled", true)
      .config(
        "spark.shuffle.sort.io.plugin.class",
        "org.apache.spark.shuffle.pmem.PlasmaShuffleDataIO"
      )
      .getOrCreate()

    import spark.sqlContext.implicits._

    val textFile = spark.read.textFile("examples/src/main/resources/people.txt")
    val wordCounts = textFile.flatMap(line => line.split(" ")).groupByKey(identity).count()
    val result = wordCounts.collect()

    // scalastyle:off println
    println(result.toSeq.mkString(" "))
    // scalastyle:off println

    spark.stop()
  }

}
