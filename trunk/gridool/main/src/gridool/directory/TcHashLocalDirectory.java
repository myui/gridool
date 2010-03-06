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
package gridool.directory;

import gridool.locking.LockManager;
import gridool.util.GridUtils;

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
import xbird.config.Settings;
import xbird.storage.DbException;
import xbird.storage.index.BTreeCallback;
import xbird.storage.index.Value;
import xbird.storage.indexer.IndexQuery;
import xbird.util.lang.ArrayUtils;
import xbird.util.primitive.Primitives;

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
    private static final int RECORD_MMAP_SIZE;
    private static final boolean USE_DEFLATE;
    static {
        RECORD_MMAP_SIZE = Primitives.parseInt(Settings.get("gridool.directory.ld.tokyocabinet.xms"), -1);
        USE_DEFLATE = Boolean.parseBoolean(Settings.get("gridool.directory.ld.tokyocabinet.enable_deflate"));
    }

    private final ConcurrentMap<String, HDB> map;

    public TcHashLocalDirectory(LockManager lockManger) {
        super(lockManger);
        this.map = new ConcurrentHashMap<String, HDB>(16);
    }

    @SuppressWarnings("unchecked")
    public HDB getInternalIndex(String idxName) {
        return map.get(idxName);
    }

    public void start() throws DbException {
        HDB idx = createIndex(DEFAULT_IDX_NAME);
        map.put(DEFAULT_IDX_NAME, idx);
    }

    public void close() throws DbException {
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

    public boolean create(String idxName) throws DbException {
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

    public void drop(String... idxNames) throws DbException {
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

    public void sync(String... idxNames) throws DbException {
        for(String idx : idxNames) {
            final HDB tch = map.get(idx);
            if(tch != null) {
                synchronized(tch) {
                    tch.sync();
                }
            }
        }
    }

    public void purgeAll(boolean clear) throws DbException {
        for(HDB tch : map.values()) {
            synchronized(tch) {
                tch.sync();
            }
        }
    }

    public void addMapping(final String idxName, final byte[][] keys, final byte[] value)
            throws DbException {
        final HDB tch = openIndex(idxName);
        final byte[] wrappedValue = wrapValue(value);
        synchronized(tch) {
            for(final byte[] key : keys) {
                if(!tch.putcat(key, wrappedValue)) {
                    int ecode = tch.ecode();
                    String errmsg = "put failed: " + HDB.errmsg(ecode);
                    LOG.error(errmsg);
                    throw new DbException(errmsg);
                }
            }
        }
    }

    public void addMapping(final String idxName, final byte[][] keys, final byte[][] values)
            throws DbException {
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
                    throw new DbException(errmsg);
                }
            }
        }
    }

    public void setMapping(String idxName, byte[][] keys, byte[][] values) throws DbException {
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
                    throw new DbException(errmsg);
                }
            }
        }
    }

    public byte[][] removeMapping(String idxName, byte[] key) throws DbException {
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

    public boolean removeMapping(String idxName, byte[]... keys) throws DbException {
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

    public void exactSearch(final String idxName, final byte[] key, final BTreeCallback handler)
            throws DbException {
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

    public byte[] getValue(String idxName, byte[] key) throws DbException {
        final HDB tch = map.get(idxName);
        if(tch == null) {
            return null;
        }
        final byte[] v;
        synchronized(tch) {
            v = tch.get(key);
        }
        return v;
    }

    private HDB openIndex(final String idxName) throws DbException {
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

    private HDB createIndex(final String name) throws DbException {
        File colDir = GridUtils.getWorkDir(true);
        File idxFile = new File(colDir, name + IDX_SUFFIX_NAME);
        if(DELETE_IDX_ON_EXIT) {
            if(idxFile.exists()) {
                if(!idxFile.delete()) {
                    throw new DbException("Could not prepared a file: " + idxFile.getAbsolutePath());
                }
            }
        }
        String filePath = idxFile.getAbsolutePath();
        final HDB tch = new HDB();
        if(!tch.open(filePath, USE_DEFLATE ? (HDB.OWRITER | HDB.OCREAT | HDB.TDEFLATE)
                : (HDB.OWRITER | HDB.OCREAT))) {
            int ecode = tch.ecode();
            String errmsg = "open error: " + HDB.errmsg(ecode);
            LOG.fatal(errmsg);
            throw new DbException(errmsg);
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

    public void prefixSearch(final String idxName, final byte[] prefix, final BTreeCallback handler)
            throws DbException {
        throw new UnsupportedOperationException();
    }

    public void retrieve(String idxName, IndexQuery query, BTreeCallback callback)
            throws DbException {
        throw new UnsupportedOperationException();
    }
}
