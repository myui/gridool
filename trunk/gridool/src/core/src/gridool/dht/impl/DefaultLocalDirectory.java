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
import gridool.dht.btree.BIndexFile;
import gridool.dht.btree.BIndexMultiValueFile;
import gridool.dht.btree.CallbackHandler;
import gridool.dht.btree.IndexException;
import gridool.dht.btree.IndexQuery;
import gridool.dht.btree.Paged;
import gridool.dht.btree.Value;
import gridool.dht.btree.BasicIndexQuery.IndexConditionEQ;
import gridool.dht.btree.BasicIndexQuery.IndexConditionSW;
import gridool.locking.LockManager;
import gridool.util.GridUtils;
import gridool.util.primitive.Primitives;

import java.io.File;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
@ThreadSafe
public final class DefaultLocalDirectory extends AbstractLocalDirectory {
    private static final Log LOG = LogFactory.getLog(DefaultLocalDirectory.class);
    private static final String IDX_SUFFIX_NAME = ".bfile";

    private static final float dataCacheRatio;
    private static final float nodeCachePurgePerc, datacachePurgePerc;
    static {
        dataCacheRatio = Primitives.parseFloat(Settings.get("gridool.directory.ld.bfile.bulkload.datacache_ratio"), 0.4f);
        nodeCachePurgePerc = Primitives.parseFloat(Settings.get("gridool.directory.ld.bfile.bulkload.nodecache_purgeperc"), 0.1f);
        datacachePurgePerc = Primitives.parseFloat(Settings.get("gridool.directory.ld.bfile.bulkload.datacache_purgeperc"), 0.2f);
    }

    @Nonnull
    private final ConcurrentMap<String, BIndexFile> map;
    private BIndexFile defaultIndex;

    public DefaultLocalDirectory(@CheckForNull LockManager lockManger) {
        super(lockManger);
        this.map = new ConcurrentHashMap<String, BIndexFile>(16);
    }

    @SuppressWarnings("unchecked")
    public BIndexFile getInternalIndex(String idxName) {
        return map.get(idxName);
    }

    public void start() throws IndexException {
        BIndexFile idx = createIndex(DEFAULT_IDX_NAME, true);
        map.put(DEFAULT_IDX_NAME, idx);
        this.defaultIndex = idx;
    }

    public void close() throws IndexException {
        final Collection<BIndexFile> values = map.values();
        if(DELETE_IDX_ON_EXIT) {
            for(BIndexFile btree : values) {
                btree.drop();
            }
        } else {
            for(BIndexFile btree : values) {
                btree.close();
            }
        }
    }

    public boolean create(String idxName) throws IndexException {
        synchronized(map) {
            BIndexFile btree = map.get(idxName);
            if(btree == null) {
                btree = createIndex(idxName, false);
                btree.init(false);
                map.put(idxName, btree);
                return true;
            }
        }
        return false;
    }

    public void drop(String... idxNames) throws IndexException {
        for(String idx : idxNames) {
            final BIndexFile btree = map.remove(idx);
            if(btree != null) {
                btree.drop();
            }
        }
    }

    public void sync(String... idxNames) throws IndexException {
        for(String idx : idxNames) {
            final BIndexFile btree = map.get(idx);
            if(btree != null) {
                btree.flush(true, false);
            }
        }
    }

    public void purgeAll(boolean clear) throws IndexException {
        for(BIndexFile btree : map.values()) {
            if(btree != null) {
                btree.flush(true, clear);
            }
        }
    }

    @Override
    public byte[] get(byte[] key) throws IndexException {
        Value k = new Value(key);
        return defaultIndex.getValueBytes(k);
    }

    @Override
    public void set(byte[] key, byte[] value) throws IndexException {
        defaultIndex.putValue(new Value(key), value);
    }

    public void addMapping(final String idxName, final byte[][] keys, final byte[] value)
            throws IndexException {
        final BIndexFile btree = openIndex(idxName);
        for(byte[] k : keys) {
            btree.addValue(new Value(k), value);
        }
    }

    public void addMapping(final String idxName, final byte[][] keys, final byte[][] values)
            throws IndexException {
        if(keys.length != values.length) {
            throw new IllegalArgumentException();
        }

        final BIndexFile btree = openIndex(idxName);
        final int length = keys.length;
        for(int i = 0; i < length; i++) {
            btree.addValue(new Value(keys[i]), values[i]);
        }
    }

    public void setMapping(String idxName, byte[][] keys, byte[][] values) throws IndexException {
        if(keys.length != values.length) {
            throw new IllegalArgumentException();
        }

        final BIndexFile btree = openIndex(idxName);
        final int length = keys.length;
        for(int i = 0; i < length; i++) {
            btree.putValue(new Value(keys[i]), values[i]);
        }
    }

    public byte[][] removeMapping(final String idxName, final byte[] key) throws IndexException {
        BIndexFile btree = map.get(idxName);
        if(btree != null) {
            Value k = new Value(key);
            return btree.remove(k);
        }
        return null;
    }

    public boolean removeMapping(final String idxName, final byte[]... keys) throws IndexException {
        final BIndexFile btree = map.get(idxName);
        if(btree != null) {
            boolean foundAll = true;
            for(byte[] key : keys) {
                Value k = new Value(key);
                if(btree.remove(k) == null) {
                    foundAll = false;
                }
            }
            return foundAll;
        }
        return false;
    }

    public void exactSearch(final String idxName, final byte[] key, final CallbackHandler handler)
            throws IndexException {
        final BIndexFile btree = map.get(idxName);
        if(btree == null) {
            return;
        }
        Value k = new Value(key);
        IndexQuery query = new IndexConditionEQ(k);
        btree.search(query, handler);
    }

    public void prefixSearch(final String idxName, final byte[] key, final CallbackHandler handler)
            throws IndexException {
        final BIndexFile btree = map.get(idxName);
        if(btree == null) {
            return;
        }
        Value k = new Value(key);
        IndexQuery query = new IndexConditionSW(k);
        btree.search(query, handler);
    }

    public void retrieve(String idxName, IndexQuery query, CallbackHandler callback)
            throws IndexException {
        final BIndexFile btree = map.get(idxName);
        if(btree == null) {
            return;
        }
        btree.search(query, callback);
    }

    private BIndexFile openIndex(final String idxName) throws IndexException {
        BIndexFile btree;
        synchronized(map) {
            btree = map.get(idxName);
            if(btree == null) {
                btree = createIndex(idxName, false);
                btree.init(false);
                map.put(idxName, btree);
            }
        }
        return btree;
    }

    private BIndexFile createIndex(final String name, final boolean create) throws IndexException {
        File colDir = GridUtils.getIndexDir(true);
        File idxFile = new File(colDir, name + IDX_SUFFIX_NAME);
        Integer cacheSize = getCacheSize(name);
        final BIndexFile btree;
        if(cacheSize == null) {
            btree = new BIndexMultiValueFile(idxFile); //new BIndexFile(idxFile, true);
        } else {
            int idxCaches = cacheSize.intValue();
            int dataCaches = (int) (idxCaches * dataCacheRatio);
            //btree = new BIndexFile(idxFile, Paged.DEFAULT_PAGESIZE, idxCaches, dataCaches, true);
            btree = new BIndexMultiValueFile(idxFile, Paged.DEFAULT_PAGESIZE, idxCaches, dataCaches);
        }
        if(DELETE_IDX_ON_EXIT) {
            if(idxFile.exists()) {
                if(!idxFile.delete()) {
                    throw new IndexException("Could not prepared a file: "
                            + idxFile.getAbsolutePath());
                }
            }
        }
        if(create) {
            btree.init(false);
        }
        LOG.info("Use an original B+tree index on " + idxFile.getAbsolutePath()
                + " for LocalDirectory");
        return btree;
    }

    @Override
    public void setBulkloading(boolean enable, String... idxNames) {
        if(idxNames.length == 0) {
            for(BIndexFile bfile : map.values()) {
                if(bfile != null) {
                    bfile.setBulkloading(enable, nodeCachePurgePerc, datacachePurgePerc);
                }
            }
        } else {
            for(String idxName : idxNames) {
                BIndexFile bfile = map.get(idxName);
                if(bfile != null) {
                    bfile.setBulkloading(enable, nodeCachePurgePerc, datacachePurgePerc);
                }
            }
        }
    }

}
