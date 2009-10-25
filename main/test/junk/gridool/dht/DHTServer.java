/*
 * @(#)$Id$
 *
 * Copyright 2006-2008 Makoto YUI
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Contributors:
 *     Makoto YUI - initial implementation
 */
package gridool.dht;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import gridool.dht.construct.DHTEntry;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
@ThreadSafe
public final class DHTServer implements DHT {

    private final ConcurrentMap<String, DHTEntry> cache;

    public DHTServer() {
        this.cache = new ConcurrentHashMap<String, DHTEntry>(128);
    }

    @Override
    public <T extends Serializable> void add(@Nonnull String[] keys, @Nonnull T value) {
        for(String key : keys) {
            DHTEntry entry = cache.get(key);
            if(entry == null) {
                final DHTEntry newEntry = new DHTEntry(value);
                final DHTEntry prevEntry = cache.putIfAbsent(key, newEntry);
                if(prevEntry == null) {
                    entry = newEntry;
                } else {
                    entry = prevEntry;
                }
            }
            entry.addValue(value);
        }
    }

    // --------------------------------------

    @Nullable
    public DHTEntry getEntry(@Nonnull String key) {
        final DHTEntry entry = cache.get(key);
        if(entry != null) {
            return entry;
        }
        return null;
    }

    // --------------------------------------

    @Override
    public boolean flush() throws IOException {
        throw new UnsupportedOperationException("Not yet supported.");
    }

    @Override
    public void close() {
        cache.clear();
    }

}
