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

import java.io.File;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.storage.DbCollection;
import xbird.storage.DbException;
import xbird.storage.index.BIndexFile;
import xbird.storage.index.BTreeCallback;
import xbird.storage.index.Value;
import xbird.storage.indexer.IndexQuery;
import xbird.storage.indexer.BasicIndexQuery.IndexConditionEQ;
import xbird.storage.indexer.BasicIndexQuery.IndexConditionSW;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
@ThreadSafe
public final class DefaultLocalDirectory extends AbstractLocalDirectory<BIndexFile> {
    private static final Log LOG = LogFactory.getLog(DefaultLocalDirectory.class);
    private static final String IDX_SUFFIX_NAME = ".bfile";

    @Nonnull
    private final ConcurrentMap<String, BIndexFile> map;

    public DefaultLocalDirectory(@CheckForNull LockManager lockManger) {
        super(lockManger);
        this.map = new ConcurrentHashMap<String, BIndexFile>(8);
    }

    public BIndexFile getInternalIndex(String idxName) {
        return map.get(idxName);
    }

    public void start() throws DbException {
        BIndexFile idx = createIndex(DEFAULT_IDX_NAME, true);
        map.put(DEFAULT_IDX_NAME, idx);
    }

    public void close() throws DbException {
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

    public boolean create(String idxName) throws DbException {
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

    public void drop(String... idxNames) throws DbException {
        for(String idx : idxNames) {
            final BIndexFile btree = map.remove(idx);
            if(btree != null) {
                btree.drop();
            }
        }
    }

    public void sync(String... idxNames) throws DbException {
        for(String idx : idxNames) {
            final BIndexFile btree = map.get(idx);
            if(btree != null) {
                btree.flush(true, false);
            }
        }
    }

    public void addMapping(final String idxName, final byte[][] keys, final byte[] value)
            throws DbException {
        final BIndexFile btree = prepareDatabase(idxName);
        for(byte[] k : keys) {
            btree.addValue(new Value(k), value);
        }
    }

    public void addMapping(final String idxName, final byte[][] keys, final byte[][] values)
            throws DbException {
        if(keys.length != values.length) {
            throw new IllegalArgumentException();
        }

        final BIndexFile btree = prepareDatabase(idxName);
        final int length = keys.length;
        for(int i = 0; i < length; i++) {
            btree.addValue(new Value(keys[i]), values[i]);
        }
    }

    public void setMapping(String idxName, byte[][] keys, byte[][] values) throws DbException {
        if(keys.length != values.length) {
            throw new IllegalArgumentException();
        }

        final BIndexFile btree = prepareDatabase(idxName);
        final int length = keys.length;
        for(int i = 0; i < length; i++) {
            btree.putValue(new Value(keys[i]), values[i]);
        }
    }

    private BIndexFile prepareDatabase(final String idxName) throws DbException {
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

    public byte[][] removeMapping(final String idxName, final byte[] key) throws DbException {
        BIndexFile btree = map.get(idxName);
        if(btree != null) {
            Value k = new Value(key);
            return btree.remove(k);
        }
        return null;
    }

    public boolean removeMapping(final String idxName, final byte[]... keys) throws DbException {
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

    @Nonnull
    public void exactSearch(final String idxName, final byte[] key, final BTreeCallback handler)
            throws DbException {
        final BIndexFile btree = map.get(idxName);
        if(btree == null) {
            return;
        }
        Value k = new Value(key);
        IndexQuery query = new IndexConditionEQ(k);
        btree.search(query, handler);
    }

    @Nonnull
    public void prefixSearch(final String idxName, final byte[] key, final BTreeCallback handler)
            throws DbException {
        final BIndexFile btree = map.get(idxName);
        if(btree == null) {
            return;
        }
        Value k = new Value(key);
        IndexQuery query = new IndexConditionSW(k);
        btree.search(query, handler);
    }

    public void retrieve(String idxName, IndexQuery query, BTreeCallback callback)
            throws DbException {
        final BIndexFile btree = map.get(idxName);
        if(btree == null) {
            return;
        }
        btree.search(query, callback);
    }

    private static BIndexFile createIndex(final String name, final boolean create)
            throws DbException {
        DbCollection rootColl = DbCollection.getRootCollection();
        File colDir = rootColl.getDirectory();
        if(!colDir.exists()) {
            throw new DbException("Database directory not found: " + colDir.getAbsoluteFile());
        }
        File idxFile = new File(colDir, name + IDX_SUFFIX_NAME);
        BIndexFile btree = new BIndexFile(idxFile, true);
        if(DELETE_IDX_ON_EXIT) {
            if(idxFile.exists()) {
                if(!idxFile.delete()) {
                    throw new DbException("Could not prepared a file: " + idxFile.getAbsolutePath());
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
}
