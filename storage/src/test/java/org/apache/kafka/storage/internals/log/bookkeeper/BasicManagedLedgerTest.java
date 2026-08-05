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

import org.apache.bookkeeper.client.MockedBookKeeperTestCase;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class BasicManagedLedgerTest extends MockedBookKeeperTestCase {

    @Test
    public void testWriteBookie() throws Throwable {
        ManagedLedgerFactory factory = getFactory();
        ManagedLedger ledger = factory.open("testWriteBookie");
        ManagedCursor cursor = ledger.newNonDurableCursor(PositionFactory.EARLIEST, "test");
        for (int i = 0; i < 10; i++) {
            ledger.addEntry(("entry-" + i).getBytes());
        }

        List<Entry> entries = cursor.readEntries(10);
        Assertions.assertEquals(10, entries.size());
        entries.forEach(Entry::release);
        cursor.close();
        ledger.close();
    }

}
