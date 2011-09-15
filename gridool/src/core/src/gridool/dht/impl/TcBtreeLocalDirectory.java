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
package gridool.dht.impl;

import gridool.Settings;
import gridool.dht.AbstractLocalDirectory;
import gridool.dht.btree.CallbackHandler;
import gridool.dht.btree.IndexException;
import gridool.dht.btree.IndexQuery;
import gridool.dht.btree.Value;
import gridool.locking.LockManager;
import gridool.util.GridUtils;
import gridool.util.primitive.Primitives;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tokyocabinet.BDB;
import tokyocabinet.BDBCUR;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
@ThreadSafe
public final class TcBtreeLocalDirectory extends AbstractLocalDirectory {
    private static final Log LOG = LogFactory.getLog(TcBtreeLocalDirectory.class);
    private static final String IDX_SUFFIX_NAME = ".tcb";
    private static final long RECORD_MMAP_SIZE;
    private static final boolean USE_DEFLATE;
    static {
        RECORD_MMAP_SIZE = Primitives.parseLong(Settings.get("gridool.directory.ld.tokyocabinet.xms"), -1L);
        USE_DEFLATE = Boolean.parseBoolean(Settings.get("gridool.directory.ld.tokyocabinet.enable_deflate"));
    }

    private final Map<String, BDB> map;
    private BDB defaultIndex;

    public TcBtreeLocalDirectory(LockManager lockManger) {
        super(lockManger);
        this.map = new ConcurrentHashMap<String, BDB>(16);
    }

    @SuppressWarnings("unchecked")
    public BDB getInternalIndex(String idxName) {
        return map.get(idxName);
    }

    public void start() throws IndexException {
        BDB idx = createIndex(DEFAULT_IDX_NAME);
        map.put(DEFAULT_IDX_NAME, idx);
        this.defaultIndex = idx;
    }

    public void close() throws IndexException {
        final Collection<BDB> values = map.values();
        for(final BDB tcb : values) {
            synchronized(tcb) {
                final String fp = tcb.path();
                if(tcb.close() && DELETE_IDX_ON_EXIT) {
                    File file = new File(fp);
                    if(file.exists()) {
                        file.delete();
                    }
                } else {
                    int ecode = tcb.ecode();
                    LOG.error("close error: " + BDB.errmsg(ecode));
                }
            }
        }
    }

    public boolean create(String idxName) throws IndexException {
        synchronized(map) {
            BDB tcb = map.get(idxName);
            if(tcb == null) {
                tcb = createIndex(idxName);
                map.put(idxName, tcb);
                return true;
            }
        }
        return false;
    }

    public void drop(String... idxNames) throws IndexException {
        for(String idx : idxNames) {
            final BDB tcb = map.remove(idx);
            if(tcb == null) {
                continue;
            }
            synchronized(tcb) {
                final String fp = tcb.path();
                if(tcb.close()) {
                    File file = new File(fp);
                    if(file.exists()) {
                        file.delete();
                    }
                }
            }
        }
    }

    public void sync(String... idxNames) throws IndexException {
        for(String idx : idxNames) {
            final BDB tcb = map.get(idx);
            if(tcb != null) {
                synchronized(tcb) {
                    tcb.sync();
                }
            }
        }
    }

    public void purgeAll(boolean clear) throws IndexException {
        for(final BDB tcb : map.values()) {
            synchronized(tcb) {
                tcb.sync();
            }
        }
    }

    @Override
    public byte[] get(byte[] key) throws IndexException {
        return defaultIndex.get(key);
    }

    @Override
    public void set(byte[] key, byte[] value) throws IndexException {
        defaultIndex.put(key, value);
    }

    public void addMapping(final String idxName, final byte[][] keys, final byte[] value)
            throws IndexException {
        final BDB tcb = openIndex(idxName);
        synchronized(tcb) {
            for(byte[] key : keys) {
                if(!tcb.putdup(key, value)) {
                    int ecode = tcb.ecode();
                    String errmsg = "put failed: " + BDB.errmsg(ecode);
                    LOG.error(errmsg);
                    throw new IndexException(errmsg);
                }
            }
        }
    }

    public void addMapping(final String idxName, final byte[][] keys, final byte[][] values)
            throws IndexException {
        if(keys.length != values.length) {
            throw new IllegalArgumentException();
        }
        final BDB tcb = openIndex(idxName);
        final int length = keys.length;
        synchronized(tcb) {
            for(int i = 0; i < length; i++) {
                if(!tcb.putdup(keys[i], values[i])) {
                    int ecode = tcb.ecode();
                    String errmsg = "put failed: " + BDB.errmsg(ecode);
                    LOG.error(errmsg);
                    throw new IndexException(errmsg);
                }
            }
        }
    }

    public void setMapping(String idxName, byte[][] keys, byte[][] values) throws IndexException {
        if(keys.length != values.length) {
            throw new IllegalArgumentException();
        }
        final BDB tcb = openIndex(idxName);
        final int length = keys.length;
        synchronized(tcb) {
            for(int i = 0; i < length; i++) {
                if(!tcb.put(keys[i], values[i])) {
                    int ecode = tcb.ecode();
                    String errmsg = "put failed: " + BDB.errmsg(ecode);
                    LOG.error(errmsg);
                    throw new IndexException(errmsg);
                }
            }
        }
    }

    public byte[][] removeMapping(String idxName, byte[] key) throws IndexException {
        final BDB tcb = map.get(idxName);
        if(tcb != null) {
            synchronized(tcb) {
                final BDBCUR cur = new BDBCUR(tcb);
                if(cur.jump(key)) {
                    final List<byte[]> list = new ArrayList<byte[]>(4);

                    byte[] k;
                    do {
                        byte[] v = cur.val();
                        list.add(v);
                        cur.out();
                        k = cur.key();
                    } while(k != null && Arrays.equals(k, key));

                    byte[][] ary = new byte[list.size()][];
                    return list.toArray(ary);
                }
            }
        }
        return null;
    }

    public boolean removeMapping(String idxName, byte[]... keys) throws IndexException {
        final BDB tcb = map.get(idxName);
        if(tcb != null) {
            synchronized(tcb) {
                final BDBCUR cur = new BDBCUR(tcb);
                boolean foundAll = true;
                for(byte[] key : keys) {
                    if(cur.jump(key)) {
                        byte[] k;
                        do {
                            cur.out();
                            k = cur.key();
                        } while(k != null && Arrays.equals(k, key));
                    } else {
                        foundAll = false;
                    }
                }
                return foundAll;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    public void exactSearch(final String idxName, final byte[] key, final CallbackHandler handler)
            throws IndexException {
        final BDB tcb = map.get(idxName);
        if(tcb == null) {
            return;
        }

        final List<byte[]> results;
        synchronized(tcb) {
            results = tcb.getlist(key);
        }
        if(results == null) {
            return;
        }
        final Value k = new Value(key);
        for(byte[] v : results) {
            handler.indexInfo(k, v);
        }
    }

    @SuppressWarnings("unchecked")
    public void prefixSearch(final String idxName, final byte[] prefix, final CallbackHandler handler)
            throws IndexException {
        final BDB tcb = map.get(idxName);
        if(tcb == null) {
            return;
        }

        final byte[] rightEdge = calculateRightEdgeValue(prefix);
        final List<byte[]> matchedKeys;
        synchronized(tcb) {
            matchedKeys = tcb.range(prefix, true, rightEdge, false, -1);
        }
        if(matchedKeys.isEmpty()) {
            return;
        }

        synchronized(tcb) {
            for(byte[] k : matchedKeys) {
                final List<byte[]> values = tcb.getlist(k);
                for(byte[] v : values) {
                    handler.indexInfo(new Value(k), v);
                }
            }
        }
    }

    public void retrieve(String idxName, IndexQuery query, CallbackHandler callback)
            throws IndexException {
        final BDB tcb = map.get(idxName);
        if(tcb == null) {
            return;
        }

        // traverse records
        synchronized(tcb) {
            final BDBCUR cur = new BDBCUR(tcb);
            cur.first();
            byte[] key;
            while((key = cur.key()) != null) {
                final Value k = new Value(key);
                if(query.testValue(k)) {
                    byte[] v = cur.val();
                    callback.indexInfo(k, v);
                }
                cur.next();
            }
        }
    }

    private BDB openIndex(final String idxName) throws IndexException {
        BDB tcb;
        synchronized(map) {
            tcb = map.get(idxName);
            if(tcb == null) {
                tcb = createIndex(idxName);
                map.put(idxName, tcb);
            }
        }
        return tcb;
    }

    private BDB createIndex(final String name) throws IndexException {
        File colDir = GridUtils.getIndexDir(true);
        File idxFile = new File(colDir, name + IDX_SUFFIX_NAME);
        if(DELETE_IDX_ON_EXIT) {
            if(idxFile.exists()) {
                if(!idxFile.delete()) {
                    throw new IndexException("Could not prepared a file: "
                            + idxFile.getAbsolutePath());
                }
            }
        }
        String filePath = idxFile.getAbsolutePath();
        final BDB tcb = new BDB();
        if(!tcb.open(filePath, USE_DEFLATE ? (BDB.OWRITER | BDB.OCREAT | BDB.TDEFLATE)
                : (BDB.OWRITER | BDB.OCREAT))) {
            int ecode = tcb.ecode();
            String errmsg = "open error: " + BDB.errmsg(ecode);
            LOG.fatal(errmsg);
            throw new IndexException(errmsg);
        } else {
            LOG.info("Use a TokyoCabinet B+tree index on " + filePath + " for LocalDirectory");
        }
        Integer cacheSize = getCacheSize(name);
        if(cacheSize != null) {
            int leafCaches = cacheSize.intValue();
            int nonLeafCaches = (int) (cacheSize.intValue() * 0.6);
            tcb.setcache(leafCaches, nonLeafCaches);
        }
        if(RECORD_MMAP_SIZE > 0) {
            tcb.setxmsiz(RECORD_MMAP_SIZE);
        }
        return tcb;
    }

    private static final byte[] calculateRightEdgeValue(final byte[] v) {
        final int vlen = v.length;
        final byte[] b = new byte[vlen + 1];
        System.arraycopy(v, 0, b, 0, vlen);
        b[vlen] = Byte.MAX_VALUE;
        return b;
    }

}
