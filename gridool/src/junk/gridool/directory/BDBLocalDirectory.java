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

import gridool.locking.LockManager;
import gridool.util.GridUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import xbird.config.Settings;
import xbird.storage.DbException;
import xbird.storage.index.CallbackHandler;
import xbird.storage.index.Value;
import xbird.storage.indexer.IndexQuery;
import xbird.util.lang.ArrayUtils;
import xbird.util.primitive.Primitives;

import com.sleepycat.db.Cursor;
import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.DatabaseType;
import com.sleepycat.db.Environment;
import com.sleepycat.db.EnvironmentConfig;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.MultipleDataEntry;
import com.sleepycat.db.MultipleKeyDataEntry;
import com.sleepycat.db.OperationStatus;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class BDBLocalDirectory extends AbstractLocalDirectory {
    private static final String IDX_SUFFIX_NAME = ".bdb";
    private static final long cacheSize = Primitives.parseLong(Settings.get("gridool.dht.ld.bdb.cachesize"), 268435456);

    private final Map<String, Database> map;

    public BDBLocalDirectory(LockManager lockManager) {
        super(lockManager);
        this.map = new ConcurrentHashMap<String, Database>(16);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Database getInternalIndex(String idxName) {
        return map.get(idxName);
    }

    @Override
    public void start() throws DbException {
        Database idx = createIndex(DEFAULT_IDX_NAME);
        map.put(DEFAULT_IDX_NAME, idx);
    }

    @Override
    public void close() throws DbException {
        for(Database db : map.values()) {
            synchronized(db) {
                try {
                    db.close(/* noSync */true);
                    if(DELETE_IDX_ON_EXIT) {
                        String fileName = db.getDatabaseFile();
                        File colDir = GridUtils.getWorkDir(true);
                        File file = new File(colDir, fileName);
                        if(file.exists()) {
                            file.delete();
                        }
                    }
                } catch (DatabaseException e) {
                    throw new DbException(e);
                }
            }
        }
    }

    @Override
    public boolean create(String idxName) throws DbException {
        synchronized(map) {
            Database db = map.get(idxName);
            if(db == null) {
                db = createIndex(idxName);
                map.put(idxName, db);
                return true;
            }
        }
        return false;
    }

    @Override
    public void drop(String... idxNames) throws DbException {
        for(String idx : idxNames) {
            final Database db = map.remove(idx);
            if(db == null) {
                continue;
            }
            synchronized(db) {
                final String fileName;
                try {
                    db.close(/* noSync */true);
                    fileName = db.getDatabaseFile();
                } catch (DatabaseException e) {
                    throw new DbException(e);
                }
                File colDir = GridUtils.getWorkDir(true);
                File file = new File(colDir, fileName);
                if(file.exists()) {
                    file.delete();
                }
            }
        }
    }

    @Override
    public void sync(String... idxNames) throws DbException {
        for(String idx : idxNames) {
            final Database db = map.get(idx);
            if(db != null) {
                synchronized(db) {
                    try {
                        db.sync();
                    } catch (DatabaseException e) {
                        throw new DbException(e);
                    }
                }
            }
        }
    }

    @Override
    public void purgeAll(boolean clear) throws DbException {
        for(final Database db : map.values()) {
            synchronized(db) {
                try {
                    db.sync();
                } catch (DatabaseException e) {
                    throw new DbException(e);
                }
            }
        }
    }

    @Override
    public void addMapping(String idxName, byte[][] keys, byte[] value) throws DbException {
        final MultipleKeyDataEntry entry = new MultipleKeyDataEntry();
        for(byte[] k : keys) {
            try {
                entry.append(k, value);
            } catch (DatabaseException e) {
                throw new DbException(e);
            }
        }

        final Database db = openIndex(idxName);
        try {
            db.putMultipleKey(null, entry, false);
        } catch (DatabaseException e) {
            throw new DbException(e);
        }
    }

    @Override
    public void addMapping(String idxName, byte[][] keys, byte[][] values) throws DbException {
        if(keys.length != values.length) {
            throw new IllegalArgumentException();
        }
        final MultipleKeyDataEntry entry = new MultipleKeyDataEntry();
        for(int i = 0; i < keys.length; i++) {
            try {
                entry.append(keys[i], values[i]);
            } catch (DatabaseException e) {
                throw new DbException(e);
            }
        }
        final Database db = openIndex(idxName);
        try {
            db.putMultipleKey(null, entry, false);
        } catch (DatabaseException e) {
            throw new DbException(e);
        }
    }

    @Override
    public void setMapping(String idxName, byte[][] keys, byte[][] values) throws DbException {
        if(keys.length != values.length) {
            throw new IllegalArgumentException();
        }
        final MultipleKeyDataEntry entry = new MultipleKeyDataEntry();
        for(int i = 0; i < keys.length; i++) {
            try {
                entry.append(keys[i], values[i]);
            } catch (DatabaseException e) {
                throw new DbException(e);
            }
        }
        final Database db = openIndex(idxName);
        try {
            db.putMultipleKey(null, entry, true);
        } catch (DatabaseException e) {
            throw new DbException(e);
        }
    }

    @Override
    public byte[][] removeMapping(String idxName, byte[] key) throws DbException {
        final Database db = map.get(idxName);
        if(db != null) {
            DatabaseEntry theKey = new DatabaseEntry(key);
            DatabaseEntry theData = new DatabaseEntry();
            Cursor cursor = null;
            try {
                cursor = db.openCursor(null, null);
                OperationStatus retVal = cursor.getSearchKey(theKey, theData, LockMode.DEFAULT);
                int matches = cursor.count();
                if(matches > 0) {
                    final byte[][] removed = new byte[matches][];
                    for(int i = 0; retVal == OperationStatus.SUCCESS; i++) {
                        byte[] v = theData.getData();
                        removed[i] = v;
                        retVal = cursor.getNextDup(theKey, theData, LockMode.DEFAULT);
                    }
                    return removed;
                }
            } catch (DatabaseException e) {
                throw new DbException(e);
            } finally {
                if(cursor != null) {
                    try {
                        cursor.close();
                    } catch (DatabaseException e) {
                        ;
                    }
                }
            }
        }
        return null;
    }

    @Override
    public boolean removeMapping(String idxName, byte[]... keys) throws DbException {
        final MultipleDataEntry keyEntry = new MultipleDataEntry();
        for(byte[] k : keys) {
            try {
                keyEntry.append(k);
            } catch (DatabaseException e) {
                throw new DbException(e);
            }
        }
        final Database db = map.get(idxName);
        if(db != null) {
            final OperationStatus status;
            try {
                status = db.deleteMultiple(null, keyEntry);
            } catch (DatabaseException e) {
                return false;
            }
            return status == OperationStatus.SUCCESS;
        }
        return false;
    }

    @Override
    public void exactSearch(String idxName, byte[] key, CallbackHandler handler) throws DbException {
        final Database db = map.get(idxName);
        if(db == null) {
            return;
        }
        DatabaseEntry theKey = new DatabaseEntry(key);
        DatabaseEntry theData = new DatabaseEntry();
        Cursor cursor = null;
        try {
            cursor = db.openCursor(null, null);
            OperationStatus retVal = cursor.getSearchKey(theKey, theData, LockMode.DEFAULT);
            if(retVal != OperationStatus.SUCCESS) {
                return;
            }
            final Value k = new Value(key);
            do {
                byte[] v = theData.getData();
                handler.indexInfo(k, v);
                retVal = cursor.getNextDup(theKey, theData, LockMode.DEFAULT);
            } while(retVal == OperationStatus.SUCCESS);
        } catch (DatabaseException e) {
            throw new DbException(e);
        } finally {
            if(cursor != null) {
                try {
                    cursor.close();
                } catch (DatabaseException e) {
                    ;
                }
            }
        }
    }

    @Override
    public void prefixSearch(String idxName, byte[] key, CallbackHandler handler)
            throws DbException {
        final Database db = map.get(idxName);
        if(db == null) {
            return;
        }
        DatabaseEntry theKey = new DatabaseEntry(key);
        DatabaseEntry theData = new DatabaseEntry();
        Cursor cursor = null;
        try {
            cursor = db.openCursor(null, null);
            OperationStatus retVal = cursor.getSearchKeyRange(theKey, theData, LockMode.DEFAULT);
            while(retVal == OperationStatus.SUCCESS) {
                byte[] k = theKey.getData();
                byte[] v = theData.getData();
                if(!ArrayUtils.startsWith(k, key)) {
                    break;
                }
                handler.indexInfo(new Value(k), v);
                retVal = cursor.getNext(theKey, theData, LockMode.DEFAULT);
            }
        } catch (DatabaseException e) {
            throw new DbException(e);
        } finally {
            if(cursor != null) {
                try {
                    cursor.close();
                } catch (DatabaseException e) {
                    ;
                }
            }
        }
    }

    @Override
    public void retrieve(String idxName, IndexQuery query, CallbackHandler callback)
            throws DbException {
        final Database db = map.get(idxName);
        if(db == null) {
            return;
        }
        DatabaseEntry foundKey = new DatabaseEntry();
        DatabaseEntry foundData = new DatabaseEntry();
        Cursor cursor = null;
        try {
            cursor = db.openCursor(null, null);
            while(cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                Value k = new Value(foundKey.getData());
                if(query.testValue(k)) {
                    byte[] v = foundData.getData();
                    callback.indexInfo(k, v);
                }
            }
        } catch (DatabaseException e) {
            throw new DbException(e);
        } finally {
            if(cursor != null) {
                try {
                    cursor.close();
                } catch (DatabaseException e) {
                    ;
                }
            }
        }
    }

    @Override
    public byte[] getValue(String idxName, byte[] key) throws DbException {
        final Database db = map.get(idxName);
        if(db == null) {
            return null;
        }
        DatabaseEntry theKey = new DatabaseEntry(key);
        DatabaseEntry theData = new DatabaseEntry();
        final OperationStatus status;
        try {
            status = db.get(null, theKey, theData, LockMode.DEFAULT);
        } catch (DatabaseException e) {
            throw new DbException(e);
        }
        return status == OperationStatus.SUCCESS ? theData.getData() : null;
    }

    private Database openIndex(final String idxName) throws DbException {
        Database db;
        synchronized(map) {
            db = map.get(idxName);
            if(db == null) {
                db = createIndex(idxName);
                map.put(idxName, db);
            }
        }
        return db;
    }

    private Database createIndex(final String name) throws DbException {
        File colDir = GridUtils.getWorkDir(true);
        String fileName = name + IDX_SUFFIX_NAME;
        File idxFile = new File(colDir, fileName);
        if(DELETE_IDX_ON_EXIT) {
            if(idxFile.exists()) {
                if(!idxFile.delete()) {
                    throw new DbException("Could not prepared a file: " + idxFile.getAbsolutePath());
                }
            }
        }
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setInitializeCache(true);
        envConfig.setTransactional(false);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setType(DatabaseType.BTREE);
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(true);
        dbConfig.setCacheSize(cacheSize);
        final Database db;
        try {
            Environment env = new Environment(colDir, envConfig);
            db = env.openDatabase(/*txn*/null, fileName, name, dbConfig);
        } catch (FileNotFoundException fe) {
            throw new DbException(fe);
        } catch (DatabaseException de) {
            throw new DbException(de);
        }
        return db;
    }
}
