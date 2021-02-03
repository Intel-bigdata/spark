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

import java.io.InputStream
import java.io.IOException
import java.io.SequenceInputStream
import java.nio.ByteBuffer
import java.nio.channels.{Channels, ReadableByteChannel, WritableByteChannel}
import java.util.{ArrayList, Collections, Enumeration, List}

import org.apache.spark.io.pmem.{PlasmaInputStream, PlasmaUtils}
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.util.AbstractFileRegion
import org.apache.spark.network.util.TransportConf

private[spark] class PlasmaInputSteamManagedBuffer(transportConf: TransportConf)
  extends ManagedBuffer {
  private val streams: List[PlasmaInputStream] = new ArrayList[PlasmaInputStream]
  private val streamIds: List[String] = new ArrayList[String]
  private val curStreamIdx: Int = 0
  private var totalLength: Long = 0L

  override def size(): Long = {
    totalLength
  }

  override def nioByteBuffer(): ByteBuffer = {
    // For current API in PlasmaUtils.getObjAsByteBuffer(),
    // we need to concatenate all the ByteBuffers of all the PlasmaInputStream
    import scala.collection.JavaConverters._
    var length = 0;
    var bbs = new ArrayList[ByteBuffer]()
    for ( streamId <- streamIds.asScala) {
      val bb = PlasmaUtils.getObjAsByteBuffer(streamId)
      bbs.add(bb)
      length += bb.remaining()
    }
    val concatBb = ByteBuffer.allocate(length)
    for ( binStream : ByteBuffer <- bbs.asScala) {
      concatBb.put(binStream)
    }
    concatBb
  }

  override def createInputStream(): InputStream = {
    val enmStreams: Enumeration[PlasmaInputStream] = Collections.enumeration(streams)
    val resultStream: SequenceInputStream = new SequenceInputStream(enmStreams)
    return resultStream
  }

  override def retain(): ManagedBuffer = {
    this
  }

  override def release(): ManagedBuffer = {
    this
  }

  @throws[IOException]
  def addStream(streamObjId: String, stream: PlasmaInputStream): Unit = {
    streamIds.add(streamObjId)
    streams.add(stream)
    totalLength += stream.available()
  }

  override def convertToNetty(): AnyRef = {
    val stream = createInputStream()
    val channel = Channels.newChannel(stream)
    new ReadableChannelFileRegion(channel, totalLength)
  }

  private class ReadableChannelFileRegion(source: ReadableByteChannel, size: Long)
    extends AbstractFileRegion {

    private var _transferred = 0L

    private val buffer = ByteBuffer.allocateDirect(64 * 1024)
    buffer.flip()

    override def count(): Long = size

    override def position(): Long = 0

    override def transferred(): Long = _transferred

    override def transferTo(target: WritableByteChannel, pos: Long): Long = {
      assert(pos == transferred(), "Invalid position.")

      var written = 0L
      var lastWrite = -1L
      while (lastWrite != 0) {
        if (!buffer.hasRemaining()) {
          buffer.clear()
          source.read(buffer)
          buffer.flip()
        }
        if (buffer.hasRemaining()) {
          lastWrite = target.write(buffer)
          written += lastWrite
        } else {
          lastWrite = 0
        }
      }

      _transferred += written
      written
    }

    override def deallocate(): Unit = source.close()
  }
}
