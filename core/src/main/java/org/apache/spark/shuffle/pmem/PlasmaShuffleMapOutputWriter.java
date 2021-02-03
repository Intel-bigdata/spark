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

package org.apache.spark.shuffle.pmem;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.apache.spark.shuffle.api.WritableByteChannelWrapper;
import org.apache.spark.shuffle.api.metadata.MapOutputCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Optional;

public class PlasmaShuffleMapOutputWriter implements ShuffleMapOutputWriter {
    private static final Logger log =
            LoggerFactory.getLogger(PlasmaShuffleMapOutputWriter.class);

    private final int shuffleId;
    private final long mapId;
    private final PlasmaShuffleBlockResolver blockResolver;
    private final SparkConf conf;
    private final long[] partitionLengths;
    private int lastPartitionId = -1;

    public PlasmaShuffleMapOutputWriter(
            int shuffleId,
            long mapId,
            int numPartitions,
            PlasmaShuffleBlockResolver blockResolver,
            SparkConf sparkConf) {
        this.shuffleId = shuffleId;
        this.mapId = mapId;
        this.blockResolver = blockResolver;
        this.partitionLengths = new long[numPartitions];
        this.conf = sparkConf;
    }

    @Override
    public ShufflePartitionWriter getPartitionWriter(int reducePartitionId) throws IOException {
        if (reducePartitionId <= lastPartitionId) {
            throw new IllegalArgumentException("Partitions should be requested in increasing order.");
        }
        lastPartitionId = reducePartitionId;
        return new PlasmaShuffleMapOutputWriter.PMemShufflePartitionWriter(reducePartitionId);
    }

    @Override
    public MapOutputCommitMessage commitAllPartitions() throws IOException {
        //Do nothing here, since we don't need write index file here.
        return null;
    }

    @Override
    public void abort(Throwable error) throws IOException {
      //Do nothing here.
    }

    private class PMemShufflePartitionWriter implements ShufflePartitionWriter {
        private final int partitionId;
        private PlasmaShuffleMapOutputWriter.PartitionWriterStream partStream = null;

        private PMemShufflePartitionWriter(int partitionId) {
            this.partitionId = partitionId;
        }

        @Override
        public OutputStream openStream() throws IOException {
            if (partStream == null) {
                OutputStream chunkOutput = blockResolver.getDataOutputStream(shuffleId, mapId, partitionId);
                partStream = new PartitionWriterStream(partitionId, chunkOutput);
            }
            return partStream;
        }

        @Override
        public Optional<WritableByteChannelWrapper> openChannelWrapper() throws IOException {
            if (partStream != null) {
                PlasmaShufflePartitionWritableChannel partChannel = new PlasmaShufflePartitionWritableChannel(partStream);
                return Optional.of(partChannel);
            } else {
                return Optional.empty();
            }
        }

        @Override
        public long getNumBytesWritten() {
            if (partStream != null) {
                return partStream.getCount();
            } else {
                // Assume an empty partition if stream and channel are never created
                return 0;
            }
        }
    }

    private class PlasmaShufflePartitionWritableChannel implements WritableByteChannelWrapper {
       private OutputStream partStream = null;
       private WritableByteChannel partChannel = null;
       public PlasmaShufflePartitionWritableChannel(OutputStream partOutputStream){
           partStream = partOutputStream;
       }

        @Override
        public WritableByteChannel channel() {
            if (partChannel == null) {
                partChannel = Channels.newChannel(partStream);
            }
            return partChannel;
        }

        @Override
        public void close() throws IOException {
            if (partChannel != null) {
                partChannel.close();
            }
        }
    }

    private class PartitionWriterStream extends OutputStream {
        private final int partitionId;
        private int count = 0;
        private boolean isClosed = false;
        private OutputStream partStream = null;

        PartitionWriterStream(int partitionId,OutputStream partStream) {
            this.partitionId = partitionId;
            this.partStream = partStream;
        }

        public int getCount() {
            return count;
        }

        @Override
        public void write(int b) throws IOException {
            verifyNotClosed();
            partStream.write(b);
            count++;
        }

        @Override
        public void write(byte[] buf, int pos, int length) throws IOException {
            verifyNotClosed();
            partStream.write(buf, pos, length);
            count += length;
        }

        @Override
        public void close() {
            isClosed = true;
            partitionLengths[partitionId] = count;
        }

        private void verifyNotClosed() {
            if (isClosed) {
                throw new IllegalStateException("Attempting to write to a closed block output stream.");
            }
        }
    }
}
