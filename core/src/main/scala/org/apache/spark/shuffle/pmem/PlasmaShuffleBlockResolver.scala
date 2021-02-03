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

package org.apache.spark.shuffle.pmem

import java.io.OutputStream

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.io.pmem.PlasmaInputStream
import org.apache.spark.io.pmem.PlasmaOutputStream
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.shuffle.ShuffleBlockResolver
import org.apache.spark.storage._

private[spark] class PlasmaShuffleBlockResolver(
  conf: SparkConf,
  var _blockManager: BlockManager = null)
  extends ShuffleBlockResolver
  with Logging {

  private lazy val blockManager = Option(_blockManager).getOrElse(SparkEnv.get.blockManager)

  private val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle")

  override def getBlockData(blockId: BlockId, dirs: Option[Array[String]]): ManagedBuffer = {
    val (shuffleId, mapId, startReduceId, endReduceId) = blockId match {
      case id: ShuffleBlockId =>
        (id.shuffleId, id.mapId, id.reduceId, id.reduceId + 1)
      case batchId: ShuffleBlockBatchId =>
        (batchId.shuffleId, batchId.mapId, batchId.startReduceId, batchId.endReduceId)
      case _ =>
        throw new IllegalArgumentException("unexpected shuffle block id format: " + blockId)
    }
    val resultBuffer = new PlasmaInputSteamManagedBuffer(transportConf);
    for (idx <- startReduceId to endReduceId) {
      val shuffleObjId = PlasmaShuffleUtil.generateShuffleId(shuffleId, mapId, idx)
      val in = new PlasmaInputStream(shuffleObjId)
      resultBuffer.addStream(shuffleObjId, in)
    }
    resultBuffer
  }

  override def stop(): Unit = {

  }

  override def removeDataByMap(shuffleId: Int, mapId: Long): Unit = {
    // ToDo: remove all the shuffle data
  }

  def getDataOutputStream(shuffleId: Int, mapId: Long, partitionId: Int): OutputStream = {
    val shuffleObjId = PlasmaShuffleUtil.generateShuffleId(shuffleId, mapId, partitionId)
    new PlasmaOutputStream(shuffleObjId)
  }
}
