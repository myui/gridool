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

import gridool.dht.AbstractLocalDirectory;
import gridool.locking.LockManager;
import gridool.util.GridUtils;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.babudb.lsmdb.BabuDBInsertGroup;
import org.xtreemfs.babudb.lsmdb.Database;
import org.xtreemfs.babudb.lsmdb.DatabaseManager;

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
public final class BabuLSMTreeDirectory extends AbstractLocalDirectory {
    private static final Log LOG = LogFactory.getLog(BabuLSMTreeDirectory.class);
    private static final String IDX_DIR_PREFIX = "babudb";

    @Nonnull
    private final BabuDB databaseSystem;;
    @Nonnull
    private final DatabaseManager dbm;
    @Nonnull
    private final ConcurrentMap<String, Database> map;

    public BabuLSMTreeDirectory(LockManager lockManager) {
        super(lockManager);
        File colDir = GridUtils.getIndexDir(true);
        String baseDir = colDir.getAbsolutePath();
        File workDir = GridUtils.getWorkDir(true);
        String dbLogDir = new File(workDir, "babudb_log").getAbsolutePath();
        int numThreads = 4;
        long maxLogFileSize = 1024 * 1024 * 512; // 512mb
        int checkInterval = 5 * 60; // 5 min
        SyncMode syncMode = SyncMode.ASYNC;
        int pseudoSyncWait = 0;
        int maxQ = 1000;
        boolean compression = true;
        int maxNumRecordsPerBlock = 0;
        int maxBlockFileSize = 0;
        BabuDBConfig conf = new BabuDBConfig(baseDir, dbLogDir, numThreads, maxLogFileSize, checkInterval, syncMode, pseudoSyncWait, maxQ, compression, maxNumRecordsPerBlock, maxBlockFileSize);
        final BabuDB databaseSystem;
        try {
            databaseSystem = BabuDBFactory.createBabuDB(conf);
        } catch (BabuDBException e) {
            throw new IllegalStateException(e);
        }
        this.databaseSystem = databaseSystem;
        this.dbm = databaseSystem.getDatabaseManager();
        this.map = new ConcurrentHashMap<String, Database>(16);
    }

    @SuppressWarnings("unchecked")
    public Database getInternalIndex(String idxName) {
        return map.get(idxName);
    }

    public void start() throws DbException {
        Database idx = createIndex(DEFAULT_IDX_NAME, true);
        map.put(DEFAULT_IDX_NAME, idx);
    }

    public void close() throws DbException {
        try {
            databaseSystem.getCheckpointer().checkpoint();
            databaseSystem.shutdown();
        } catch (BabuDBException e) {
            throw new DbException(e);
        } catch (InterruptedException ie) {
            throw new DbException(ie);
        }
    }

    public boolean create(String idxName) throws DbException {
        synchronized(map) {
            Database idx = map.get(idxName);
            if(idx == null) {
                idx = createIndex(idxName, false);
                map.put(idxName, idx);
                return true;
            }
        }
        return false;
    }

    public void drop(String... idxNames) throws DbException {
        for(String name : idxNames) {
            final Database idx = map.remove(name);
            if(idx != null) {
                try {
                    idx.shutdown();
                } catch (BabuDBException e) {
                    throw new DbException(e);
                }
            }
        }
    }

    // nop
    public void sync(String... idxNames) throws DbException {
        try {
            databaseSystem.getCheckpointer().checkpoint();
        } catch (BabuDBException e) {
            throw new DbException(e);
        } catch (InterruptedException ie) {
            throw new DbException(ie);
        }
    }

    public void purgeAll(boolean clear) throws DbException {
        try {
            databaseSystem.getCheckpointer().checkpoint();
        } catch (BabuDBException e) {
            throw new DbException(e);
        } catch (InterruptedException ie) {
            throw new DbException(ie);
        }
    }

    public void addMapping(String idxName, byte[][] keys, byte[] value) throws DbException {
        Database idx = openIndex(idxName);
        final BabuDBInsertGroup group;
        try {
            group = idx.createInsertGroup();
        } catch (BabuDBException e) {
            throw new DbException(e);
        }
        for(byte[] key : keys) {
            group.addInsert(0, key, value);
        }
        idx.insert(group, null);
    }

    public void addMapping(String idxName, byte[][] keys, byte[][] values) throws DbException {
        if(keys.length != values.length) {
            throw new IllegalArgumentException();
        }
        Database idx = openIndex(idxName);
        final BabuDBInsertGroup group;
        try {
            group = idx.createInsertGroup();
        } catch (BabuDBException e) {
            throw new DbException(e);
        }
        final int length = keys.length;
        for(int i = 0; i < length; i++) {
            group.addInsert(0, keys[i], values[i]);
        }
        idx.insert(group, null);
    }

    public void setMapping(String idxName, byte[][] keys, byte[][] values) throws DbException {
        if(keys.length != values.length) {
            throw new IllegalArgumentException();
        }
        Database idx = openIndex(idxName);
        final BabuDBInsertGroup group;
        try {
            group = idx.createInsertGroup();
        } catch (BabuDBException e) {
            throw new DbException(e);
        }
        final int length = keys.length;
        for(int i = 0; i < length; i++) {
            group.addInsert(0, keys[i], values[i]);
        }
        idx.insert(group, null);
    }

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

    private Database openIndex(final String idxName) throws DbException {
        Database idx;
        synchronized(map) {
            idx = map.get(idxName);
            if(idx == null) {
                idx = createIndex(idxName, false);
                map.put(idxName, idx);
            }
        }
        return idx;
    }

    private Database createIndex(final String name, final boolean create) throws DbException {
        try {
            return dbm.createDatabase(name, 1);
        } catch (BabuDBException e) {
            throw new DbException(e);
        }
    }

}
