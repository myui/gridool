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
package gridool.directory.impl;

import gridool.directory.AbstractLocalDirectory;
import gridool.locking.LockManager;
import gridool.util.GridUtils;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xtreemfs.babudb.index.DefaultByteRangeComparator;
import org.xtreemfs.babudb.index.reader.DiskIndex;
import org.xtreemfs.foundation.util.FSUtils;

import xbird.storage.DbException;
import xbird.storage.index.CallbackHandler;
import xbird.storage.indexer.IndexQuery;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class BabuDiskIndexDirectory extends AbstractLocalDirectory {
    private static final Log LOG = LogFactory.getLog(BabuDiskIndexDirectory.class);
    private static final String IDX_DIR_PREFIX = "bubu_";

    @Nonnull
    private final ConcurrentMap<String, DiskIndex> map;

    public BabuDiskIndexDirectory(LockManager lockManager) {
        super(lockManager);
        this.map = new ConcurrentHashMap<String, DiskIndex>(16);
    }

    @SuppressWarnings("unchecked")
    public DiskIndex getInternalIndex(String idxName) {
        return map.get(idxName);
    }

    public void start() throws DbException {
        DiskIndex idx = createIndex(DEFAULT_IDX_NAME, true);
        map.put(DEFAULT_IDX_NAME, idx);
    }

    public void close() throws DbException {
        final File colDir = GridUtils.getIndexDir(true);
        for(Map.Entry<String, DiskIndex> e : map.entrySet()) {
            DiskIndex idx = e.getValue();
            idx.finalize();
            if(DELETE_IDX_ON_EXIT) {
                String name = e.getKey();
                File idxDir = new File(colDir, name);
                FSUtils.delTree(idxDir);
            }
        }
    }

    public boolean create(String idxName) throws DbException {
        synchronized(map) {
            DiskIndex idx = map.get(idxName);
            if(idx == null) {
                idx = createIndex(idxName, false);
                map.put(idxName, idx);
                return true;
            }
        }
        return false;
    }

    public void drop(String... idxNames) throws DbException {
        final File colDir = GridUtils.getIndexDir(true);
        for(String name : idxNames) {
            final DiskIndex idx = map.remove(name);
            if(idx != null) {
                idx.finalize();
                File idxDir = new File(colDir, name);
                FSUtils.delTree(idxDir);
            }
        }
    }

    // nop
    public void sync(String... idxNames) throws DbException {}

    public void purgeAll(boolean clear) throws DbException {
        if(clear) {
            for(DiskIndex idx : map.values()) {
                idx.finalize();
            }            
        }
    }

    public void addMapping(String idxName, byte[][] keys, byte[] value) throws DbException {
        
    }

    public void addMapping(String idxName, byte[][] keys, byte[][] values) throws DbException {}

    public void setMapping(String idxName, byte[][] keys, byte[][] values) throws DbException {}

    @Override
    public byte[][] removeMapping(String idxName, byte[] key) throws DbException {
        return null;
    }

    @Override
    public boolean removeMapping(String idxName, byte[]... keys) throws DbException {
        return false;
    }

    public void exactSearch(String idxName, byte[] key, CallbackHandler handler) throws DbException {}

    public void prefixSearch(String idxName, byte[] key, CallbackHandler handler)
            throws DbException {}

    public void retrieve(String idxName, IndexQuery query, CallbackHandler callback)
            throws DbException {}

    private DiskIndex openIndex(final String idxName) throws DbException {
        DiskIndex idx;
        synchronized(map) {
            idx = map.get(idxName);
            if(idx == null) {
                idx = createIndex(idxName, false);
                map.put(idxName, idx);
            }
        }
        return idx;
    }

    private DiskIndex createIndex(final String name, final boolean create) throws DbException {
        File colDir = GridUtils.getIndexDir(true);
        if(DELETE_IDX_ON_EXIT) {
            FSUtils.delTree(colDir);
        }

        File idxDir = new File(colDir, IDX_DIR_PREFIX + name);
        final DiskIndex index;
        try {
            index = new DiskIndex(idxDir.getAbsolutePath(), DefaultByteRangeComparator.getInstance(), true, true);
        } catch (IOException e) {
            throw new DbException("Failed to prepare DiskIndex: " + idxDir.getAbsolutePath(), e);
        }
        LOG.info("Use a BubuDB disk index on " + idxDir.getAbsolutePath() + " for LocalDirectory");
        return index;
    }

}
