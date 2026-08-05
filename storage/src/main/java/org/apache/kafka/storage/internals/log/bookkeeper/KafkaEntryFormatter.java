/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.storage.internals.log.bookkeeper;

import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.storage.internals.log.LogAppendInfo;

import com.google.common.annotations.VisibleForTesting;

import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

public class KafkaEntryFormatter {
    private static final Logger log = LoggerFactory.getLogger(KafkaEntryFormatter.class);

    private static final Commands.ChecksumType CHECKSUM_TYPE = Commands.ChecksumType.None;

    public static ByteBuf encode(LogAppendInfo appendInfo, MemoryRecords records) {
        final ByteBuf payload = wrapByteBuffer(records.buffer());
        final MessageMetadata metadata = metadata(appendInfo);
        return serializeMetadataAndPayload(CHECKSUM_TYPE, metadata, payload);
    }

    public static MemoryRecords decode(List<Entry> entries) {
        int totalBytes = 0;
        for (Entry entry : entries) {
            ByteBuf buf = entry.getDataBuffer();
            MessageMetadataUtils.skipBrokerMessageMetadata(buf);
            MessageMetadataUtils.skipMessageMetadata(buf);
            totalBytes += buf.readableBytes();
        }
        ByteBuffer buffer = ByteBuffer.allocate(totalBytes);
        for (Entry entry : entries) {
            entry.getDataBuffer().readBytes(buffer);
            buffer.flip();
            entry.release();
        }
        return MemoryRecords.readableRecords(buffer);
    }

    private static MessageMetadata metadata(LogAppendInfo appendInfo) {
        return new MessageMetadata()
                .setPublishTime(appendInfo.maxTimestamp())
                .setNumMessagesInBatch((int) appendInfo.numMessages())
                .setProducerName("-")
                .setSequenceId(-1L);
    }

    private static ByteBuf wrapByteBuffer(ByteBuffer payload) {
        ByteBuf copy = PulsarByteBufAllocator.DEFAULT.directBuffer(payload.remaining(), payload.remaining());
        copy.writeBytes(payload);
        return copy;
    }

    @VisibleForTesting
    public static ByteBuf serializeMetadataAndPayload(Commands.ChecksumType checksumType,
                                                      MessageMetadata msgMetadata, ByteBuf payload) {
        // / Wire format
        // [MAGIC_NUMBER][CHECKSUM] [METADATA_SIZE][METADATA] [PAYLOAD]
        int msgMetadataSize = msgMetadata.getSerializedSize();
        int magicAndChecksumLength = 0;
        int headerContentSize = magicAndChecksumLength + 4 + msgMetadataSize; // magicLength +
        // checksumSize + msgMetadataLength +
        // msgMetadataSize

        ByteBuf header = PulsarByteBufAllocator.DEFAULT.buffer(headerContentSize, headerContentSize);

        // Write metadata
        header.writeInt(msgMetadataSize);
        msgMetadata.writeTo(header);

        CompositeByteBuf headerAndPayload = PulsarByteBufAllocator.DEFAULT.compositeBuffer();
        headerAndPayload.addComponent(true, header);
        headerAndPayload.addComponent(true, payload);
        return headerAndPayload;
    }
}
