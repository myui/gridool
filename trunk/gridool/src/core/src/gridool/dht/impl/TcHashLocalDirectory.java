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
import gridool.util.lang.ArrayUtils;
import gridool.util.primitive.Primitives;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tokyocabinet.HDB;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
@ThreadSafe
public final class TcHashLocalDirectory extends AbstractLocalDirectory {
    private static final Log LOG = LogFactory.getLog(TcHashLocalDirectory.class);
    private static final String IDX_SUFFIX_NAME = ".tch";
    private static final long NUM_HASH_BUCKETS;
    private static final long RECORD_MMAP_SIZE;
    private static final boolean USE_DEFLATE;
    static {
        NUM_HASH_BUCKETS = Primitives.parseLong(Settings.get("gridool.dht.ld.tokyocabinet.hashbuckets"), 2000000L);
        RECORD_MMAP_SIZE = Primitives.parseLong(Settings.get("gridool.dht.ld.tokyocabinet.xms"), -1L);
        USE_DEFLATE = Boolean.parseBoolean(Settings.get("gridool.dht.ld.tokyocabinet.enable_deflate"));
    }

    private final ConcurrentMap<String, HDB> map;
    private HDB defaultIndex;

    public TcHashLocalDirectory(LockManager lockManger) {
        super(lockManger);
        this.map = new ConcurrentHashMap<String, HDB>(16);
    }

    @SuppressWarnings("unchecked")
    public HDB getInternalIndex(String idxName) {
        return map.get(idxName);
    }

    public void start() throws IndexException {
        HDB idx = createIndex(DEFAULT_IDX_NAME);
        map.put(DEFAULT_IDX_NAME, idx);
        this.defaultIndex = idx;
    }

    public void close() throws IndexException {
        final Collection<HDB> values = map.values();
        for(final HDB tch : values) {
            synchronized(tch) {
                final String fp = tch.path();
                if(tch.close() && DELETE_IDX_ON_EXIT) {
                    File file = new File(fp);
                    if(file.exists()) {
                        file.delete();
                    }
                } else {
                    int ecode = tch.ecode();
                    LOG.error("close error: " + HDB.errmsg(ecode));
                }
            }
        }
    }

    public boolean create(String idxName) throws IndexException {
        synchronized(map) {
            HDB tch = map.get(idxName);
            if(tch == null) {
                tch = createIndex(idxName);
                map.put(idxName, tch);
                return true;
            }
        }
        return false;
    }

    public void drop(String... idxNames) throws IndexException {
        for(String idx : idxNames) {
            final HDB tch = map.remove(idx);
            if(tch == null) {
                continue;
            }
            synchronized(tch) {
                final String fp = tch.path();
                if(tch.close()) {
                    File file = new File(fp);
                    if(file.exists()) {
                        file.delete();
                    }
                } else {
                    int ecode = tch.ecode();
                    LOG.warn("Close failed: " + HDB.errmsg(ecode));
                }
            }
        }
    }

    public void sync(String... idxNames) throws IndexException {
        for(String idx : idxNames) {
            final HDB tch = map.get(idx);
            if(tch != null) {
                synchronized(tch) {
                    tch.sync();
                }
            }
        }
    }

    public void purgeAll(boolean clear) throws IndexException {
        for(HDB tch : map.values()) {
            synchronized(tch) {
                tch.sync();
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
        final HDB tch = openIndex(idxName);
        final byte[] wrappedValue = wrapValue(value);
        synchronized(tch) {
            for(final byte[] key : keys) {
                if(!tch.putcat(key, wrappedValue)) {
                    int ecode = tch.ecode();
                    String errmsg = "put failed: " + HDB.errmsg(ecode);
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
        final HDB tch = openIndex(idxName);
        synchronized(tch) {
            final int length = keys.length;
            for(int i = 0; i < length; i++) {
                final byte[] wrappedValue = wrapValue(values[i]);
                if(!tch.putcat(keys[i], wrappedValue)) {
                    int ecode = tch.ecode();
                    String errmsg = "put failed: " + HDB.errmsg(ecode);
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
        final HDB tch = openIndex(idxName);
        synchronized(tch) {
            final int length = keys.length;
            for(int i = 0; i < length; i++) {
                final byte[] wrappedValue = wrapValue(values[i]);
                if(!tch.put(keys[i], wrappedValue)) {
                    int ecode = tch.ecode();
                    String errmsg = "put failed: " + HDB.errmsg(ecode);
                    LOG.error(errmsg);
                    throw new IndexException(errmsg);
                }
            }
        }
    }

    public byte[][] removeMapping(String idxName, byte[] key) throws IndexException {
        final HDB tch = map.get(idxName);
        if(tch != null) {
            synchronized(tch) {
                byte[] v = tch.get(key);
                if(v != null) {
                    tch.out(key);
                    byte[][] unwrappedValues = unwrapValues(v);
                    return unwrappedValues;
                }
            }
        }
        return null;
    }

    public boolean removeMapping(String idxName, byte[]... keys) throws IndexException {
        final HDB tch = map.get(idxName);
        if(tch != null) {
            boolean removedAll = true;
            synchronized(tch) {
                for(byte[] key : keys) {
                    if(!tch.out(key)) {
                        removedAll = false;
                    }
                }
            }
            return removedAll;
        }
        return false;
    }

    public void exactSearch(final String idxName, final byte[] key, final CallbackHandler handler)
            throws IndexException {
        final HDB tch = map.get(idxName);
        if(tch == null) {
            return;
        }

        final byte[] results;
        synchronized(tch) {
            results = tch.get(key);
        }
        if(results == null) {
            return;
        }
        final byte[][] rlist = unwrapValues(results);
        final Value k = new Value(key);
        for(final byte[] v : rlist) {
            handler.indexInfo(k, v);
        }
    }

    private HDB openIndex(final String idxName) throws IndexException {
        HDB tch;
        synchronized(map) {
            tch = map.get(idxName);
            if(tch == null) {
                tch = createIndex(idxName);
                map.put(idxName, tch);
            }
        }
        return tch;
    }

    private HDB createIndex(final String name) throws IndexException {
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
        final HDB tch = new HDB();
        if(!tch.open(filePath, HDB.OWRITER | HDB.OCREAT)) {
            int ecode = tch.ecode();
            String errmsg = "open error: " + HDB.errmsg(ecode);
            LOG.fatal(errmsg);
            throw new IndexException(errmsg);
        } else {
            LOG.info("Use a TokyoCabinet Hash index on " + filePath + " for LocalDirectory");
        }
        Integer cacheSize = getCacheSize(name);
        if(cacheSize != null) {
            int leafCaches = cacheSize.intValue();
            tch.setcache(leafCaches);
        }
        if(RECORD_MMAP_SIZE > 0) {
            tch.setxmsiz(RECORD_MMAP_SIZE);
        }
        tch.tune(NUM_HASH_BUCKETS, /* alignment */4, /* block pool 2^N */10, USE_DEFLATE ? HDB.TDEFLATE
                : 0);
        return tch;
    }

    private static byte[] wrapValue(final byte[] b) {
        int size = b.length;
        byte[] nb = new byte[size + 4];
        Primitives.putInt(nb, 0, size);
        System.arraycopy(b, 0, nb, 4, size);
        return nb;
    }

    private static byte[][] unwrapValues(final byte[] b) {
        int total = b.length;
        int size = Primitives.getInt(b, 0);
        if((size + 4) == total) {
            byte[] fb = ArrayUtils.copyOfRange(b, 4, size);
            return new byte[][] { fb };
        } else {
            final List<byte[]> list = new ArrayList<byte[]>(4);
            byte[] b1 = ArrayUtils.copyOfRange(b, 4, size);
            list.add(b1);
            int pos = 4 + size;
            do {
                int csize = Primitives.getInt(b, pos);
                byte[] cb = ArrayUtils.copyOfRange(b, pos + 4, csize);
                list.add(cb);
                pos = pos + 4 + csize;
            } while(pos < total);
            int listlen = list.size();
            final byte[][] ret = new byte[listlen][];
            list.toArray(ret);
            return ret;
        }
    }

    public void prefixSearch(final String idxName, final byte[] prefix, final CallbackHandler handler)
            throws IndexException {
        throw new UnsupportedOperationException();
    }

    public void retrieve(String idxName, IndexQuery query, CallbackHandler callback)
            throws IndexException {
        throw new UnsupportedOperationException();
    }
}
