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
package gridool.dht.helpers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.xtreemfs.babudb.index.reader.DiskIndex;
import org.xtreemfs.babudb.index.writer.DiskIndexWriter;

import xbird.storage.index.CallbackHandler;
import xbird.storage.index.Value;
import xbird.storage.indexer.IndexQuery;
import xbird.util.lang.ArrayUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class DiskIndexOverlay {

    @Nonnull
    private final String idxDirPath;
    private final int blockEntries;
    @Nonnull
    private final Map<byte[], List<byte[]>> map;
    @Nonnull
    private final DiskIndexWriter indexWriter;
    
    @Nullable
    private DiskIndex index = null;

    public DiskIndexOverlay(String idxDirPath, int blockEntries) {
        this.idxDirPath = idxDirPath;
        this.blockEntries = blockEntries;
            this.map = new HashMap<byte[], List<byte[]>>(blockEntries);
        try {
            this.indexWriter = new DiskIndexWriter(idxDirPath, Integer.MAX_VALUE, true, Integer.MAX_VALUE);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public synchronized void add(byte[][] keys, byte[] value) {
        for(byte[] k : keys) {
            List<byte[]> list = map.get(k);
            if(list == null) {
                list = new ArrayList<byte[]>(4);
                map.put(k, list);
            }
            list.add(value);
        }
        if(map.size() >= blockEntries) {
            indexWriter.writeIndex()
        }
    }

    public synchronized void add(byte[][] keys, byte[][] values) {
        if(keys.length != values.length) {
            throw new IllegalArgumentException();
        }
        for(int i = 0; i < keys.length; i++) {
            byte[] k = keys[i];
            List<byte[]> list = map.get(k);
            if(list == null) {
                list = new ArrayList<byte[]>(4);
                map.put(k, list);
            }
            byte[] v = values[i];
            list.add(v);
        }
    }

    public synchronized void set(byte[][] keys, byte[][] values) {
        if(keys.length != values.length) {
            throw new IllegalArgumentException();
        }
        for(int i = 0; i < keys.length; i++) {
            byte[] k = keys[i];
            List<byte[]> list = new ArrayList<byte[]>(4);
            list.add(values[i]);
            map.put(k, list);
        }
    }

    public synchronized byte[][] remove(byte[] key) {
        List<byte[]> list = map.remove(key);
        return (list == null) ? null : ArrayUtils.toArray(list);
    }

    public synchronized boolean remove(byte[]... keys) {
        boolean removedAll = true;
        for(byte[] k : keys) {
            if(map.remove(k) == null) {
                removedAll = false;
            }
        }
        return removedAll;
    }

    public synchronized void exactSearch(byte[] key, CallbackHandler handler) {
        List<byte[]> list = map.get(key);
        if(list != null) {
            final Value k = new Value(key);
            for(byte[] v : list) {
                handler.indexInfo(k, v);
            }
        }
    }

    /**
     * Not supported.
     */
    public synchronized void prefixSearch(byte[] key, CallbackHandler handler) {
        throw new UnsupportedOperationException();
    }


    /**
     * Not supported.
     */
    public synchronized void retrieve(IndexQuery query, CallbackHandler callback) {
        throw new UnsupportedOperationException();
    }

}
