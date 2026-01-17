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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.kafka.common.record.MemoryRecords;

public final class EntriesDecodeResult {
    private final static ByteBuf EMPTY_BUF = Unpooled.EMPTY_BUFFER;

    public final ByteBuf buf;
    public final int numEntries;
    public final MemoryRecords records;

    public EntriesDecodeResult(ByteBuf buf, int numEntries, MemoryRecords records) {
        this.buf = buf;
        this.numEntries = numEntries;
        this.records = records;
    }

    public void release() {
        buf.release();
    }

    public static final EntriesDecodeResult EMPTY = new EntriesDecodeResult(EMPTY_BUF, 0, MemoryRecords.EMPTY);
}
