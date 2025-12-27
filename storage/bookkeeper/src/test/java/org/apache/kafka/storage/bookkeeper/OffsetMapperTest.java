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
package org.apache.kafka.storage.bookkeeper;

import org.apache.kafka.storage.bookkeeper.OffsetMapper.BookKeeperPosition;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link OffsetMapper}.
 */
class OffsetMapperTest {

    private OffsetMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new OffsetMapper();
    }

    @Test
    void testRecordAndGetMapping() {
        BookKeeperPosition pos1 = new BookKeeperPosition(1, 0);
        BookKeeperPosition pos2 = new BookKeeperPosition(1, 1);
        BookKeeperPosition pos3 = new BookKeeperPosition(2, 0);

        mapper.recordMapping(0, pos1);
        mapper.recordMapping(10, pos2);
        mapper.recordMapping(20, pos3);

        assertEquals(Optional.of(pos1), mapper.getPosition(0));
        assertEquals(Optional.of(pos1), mapper.getPosition(5)); // Floor lookup
        assertEquals(Optional.of(pos2), mapper.getPosition(10));
        assertEquals(Optional.of(pos2), mapper.getPosition(15));
        assertEquals(Optional.of(pos3), mapper.getPosition(20));
        assertEquals(Optional.of(pos3), mapper.getPosition(25));
    }

    @Test
    void testAllocateOffset() {
        BookKeeperPosition pos = new BookKeeperPosition(1, 0);
        
        assertEquals(0, mapper.getNextOffset());
        
        long offset = mapper.allocateOffset(pos);
        assertEquals(0, offset);
        assertEquals(1, mapper.getNextOffset());
        
        offset = mapper.allocateOffset(new BookKeeperPosition(1, 1));
        assertEquals(1, offset);
        assertEquals(2, mapper.getNextOffset());
    }

    @Test
    void testStartingOffset() {
        mapper = new OffsetMapper(100);
        
        assertEquals(100, mapper.getNextOffset());
        
        long offset = mapper.allocateOffset(new BookKeeperPosition(1, 0));
        assertEquals(100, offset);
        assertEquals(101, mapper.getNextOffset());
    }

    @Test
    void testTruncation() {
        mapper.recordMapping(0, new BookKeeperPosition(1, 0));
        mapper.recordMapping(10, new BookKeeperPosition(1, 1));
        mapper.recordMapping(20, new BookKeeperPosition(1, 2));
        
        assertEquals(3, mapper.size());
        
        mapper.truncateTo(10);
        
        assertEquals(1, mapper.size());
        assertEquals(10, mapper.getNextOffset());
        assertTrue(mapper.getPosition(0).isPresent());
        // After truncation, offset 10 and above should not have direct mappings
        // Floor lookup at 10 will still find offset 0's mapping, but the direct mapping is gone
        assertEquals(1, mapper.size());
    }

    @Test
    void testSetCurrentLedger() {
        assertEquals(-1, mapper.getCurrentLedgerId());
        assertEquals(-1, mapper.getCurrentEntryId());
        
        mapper.setCurrentLedger(5);
        
        assertEquals(5, mapper.getCurrentLedgerId());
        assertEquals(-1, mapper.getCurrentEntryId());
    }

    @Test
    void testClear() {
        mapper.recordMapping(0, new BookKeeperPosition(1, 0));
        mapper.recordMapping(10, new BookKeeperPosition(1, 1));
        
        mapper.clear();
        
        assertEquals(0, mapper.size());
        assertEquals(0, mapper.getNextOffset());
        assertEquals(-1, mapper.getCurrentLedgerId());
    }

    @Test
    void testCompositeOffset() {
        // Test conversion between composite offset and position
        BookKeeperPosition pos = new BookKeeperPosition(1234567890L, 987654321L);
        
        long composite = OffsetMapper.toCompositeOffset(pos);
        BookKeeperPosition recovered = OffsetMapper.fromCompositeOffset(composite);
        
        assertEquals(pos.ledgerId(), recovered.ledgerId());
        assertEquals(pos.entryId(), recovered.entryId());
    }

    @Test
    void testPositionValidity() {
        assertTrue(new BookKeeperPosition(0, 0).isValid());
        assertTrue(new BookKeeperPosition(1, 100).isValid());
        assertFalse(new BookKeeperPosition(-1, 0).isValid());
        assertFalse(new BookKeeperPosition(0, -1).isValid());
    }
}
