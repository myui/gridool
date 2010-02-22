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
import xbird.storage.DbCollection;
import xbird.storage.DbException;
import xbird.storage.index.BTreeCallback;
import xbird.storage.index.Value;
import xbird.storage.indexer.IndexQuery;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
@ThreadSafe
public final class TcbLocalDirectory extends AbstractLocalDirectory {
    private static final Log LOG = LogFactory.getLog(TcbLocalDirectory.class);
    private static final String IDX_SUFFIX_NAME = ".tcb";

    private final Map<String, BDB> map;

    public TcbLocalDirectory(LockManager lockManger) {
        super(lockManger);
        this.map = new ConcurrentHashMap<String, BDB>(8);
    }

    @SuppressWarnings("unchecked")
    public BDB getInternalIndex(String idxName) {
        return map.get(idxName);
    }

    public void start() throws DbException {
        BDB idx = createIndex(DEFAULT_IDX_NAME);
        map.put(DEFAULT_IDX_NAME, idx);
    }

    public void close() throws DbException {
        final Collection<BDB> values = map.values();
        for(BDB tcb : values) {
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

    public boolean create(String idxName) throws DbException {
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

    public void drop(String... idxNames) throws DbException {
        for(String idx : idxNames) {
            final BDB tcb = map.remove(idx);
            if(tcb == null) {
                continue;
            }
            final String fp = tcb.path();
            if(tcb.close()) {
                File file = new File(fp);
                if(file.exists()) {
                    file.delete();
                }
            }
        }
    }

    public void sync(String... idxNames) throws DbException {
        for(String idx : idxNames) {
            final BDB tcb = map.get(idx);
            if(tcb != null) {
                tcb.sync();
            }
        }
    }

    public void purgeAll(boolean clear) throws DbException {
        for(BDB tcb : map.values()) {
            tcb.sync();
        }
    }

    public void addMapping(final String idxName, final byte[][] keys, final byte[] value)
            throws DbException {
        final BDB tcb = prepareDatabase(idxName);
        for(byte[] key : keys) {
            if(!tcb.putdup(key, value)) {
                int ecode = tcb.ecode();
                String errmsg = "put failed: " + BDB.errmsg(ecode);
                LOG.error(errmsg);
                throw new DbException(errmsg);
            }
        }
    }

    public void addMapping(final String idxName, final byte[][] keys, final byte[][] values)
            throws DbException {
        if(keys.length != values.length) {
            throw new IllegalArgumentException();
        }
        final BDB tcb = prepareDatabase(idxName);
        final int length = keys.length;
        for(int i = 0; i < length; i++) {
            if(!tcb.putdup(keys[i], values[i])) {
                int ecode = tcb.ecode();
                String errmsg = "put failed: " + BDB.errmsg(ecode);
                LOG.error(errmsg);
                throw new DbException(errmsg);
            }
        }
    }

    public void setMapping(String idxName, byte[][] keys, byte[][] values) throws DbException {
        if(keys.length != values.length) {
            throw new IllegalArgumentException();
        }
        final BDB tcb = prepareDatabase(idxName);
        final int length = keys.length;
        for(int i = 0; i < length; i++) {
            if(!tcb.put(keys[i], values[i])) {
                int ecode = tcb.ecode();
                String errmsg = "put failed: " + BDB.errmsg(ecode);
                LOG.error(errmsg);
                throw new DbException(errmsg);
            }
        }
    }

    private BDB prepareDatabase(final String idxName) throws DbException {
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

    public byte[][] removeMapping(String idxName, byte[] key) throws DbException {
        final BDB tcb = map.get(idxName);
        if(tcb != null) {
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
        return null;
    }

    public boolean removeMapping(String idxName, byte[]... keys) throws DbException {
        final BDB tcb = map.get(idxName);
        if(tcb != null) {
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
        return false;
    }

    @SuppressWarnings("unchecked")
    public void exactSearch(final String idxName, final byte[] key, final BTreeCallback handler)
            throws DbException {
        final BDB tcb = map.get(idxName);
        if(tcb == null) {
            return;
        }

        final List<byte[]> results = tcb.getlist(key);
        if(results == null) {
            return;
        }
        final Value k = new Value(key);
        for(byte[] v : results) {
            handler.indexInfo(k, v);
        }
    }

    @SuppressWarnings("unchecked")
    public void prefixSearch(final String idxName, final byte[] prefix, final BTreeCallback handler)
            throws DbException {
        final BDB tcb = map.get(idxName);
        if(tcb == null) {
            return;
        }

        final byte[] rightEdge = calculateRightEdgeValue(prefix);
        final List<byte[]> matchedKeys = tcb.range(prefix, true, rightEdge, false, -1);
        if(matchedKeys.isEmpty()) {
            return;
        }

        for(byte[] k : matchedKeys) {
            final List<byte[]> values = tcb.getlist(k);
            for(byte[] v : values) {
                handler.indexInfo(new Value(k), v);
            }
        }
    }

    public void retrieve(String idxName, IndexQuery query, BTreeCallback callback)
            throws DbException {
        final BDB tcb = map.get(idxName);
        if(tcb == null) {
            return;
        }

        // traverse records
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

    private static final byte[] calculateRightEdgeValue(final byte[] v) {
        final int vlen = v.length;
        final byte[] b = new byte[vlen + 1];
        System.arraycopy(v, 0, b, 0, vlen);
        b[vlen] = Byte.MAX_VALUE;
        return b;
    }

    public byte[] getValue(String idxName, byte[] key) throws DbException {
        final BDB tcb = map.get(idxName);
        if(tcb == null) {
            return null;
        }
        byte[] v = tcb.get(key);
        return v;
    }

    private static BDB createIndex(final String name) throws DbException {
        DbCollection rootColl = DbCollection.getRootCollection();
        File colDir = rootColl.getDirectory();
        if(!colDir.exists()) {
            throw new DbException("Database directory not found: " + colDir.getAbsoluteFile());
        }
        File idxFile = new File(colDir, name + IDX_SUFFIX_NAME);
        if(DELETE_IDX_ON_EXIT) {
            if(idxFile.exists()) {
                if(!idxFile.delete()) {
                    throw new DbException("Could not prepared a file: " + idxFile.getAbsolutePath());
                }
            }
        }
        String filePath = idxFile.getAbsolutePath();
        final BDB tcb = new BDB();
        if(!tcb.open(filePath, (BDB.OWRITER | BDB.OCREAT))) {
            int ecode = tcb.ecode();
            String errmsg = "open error: " + BDB.errmsg(ecode);
            LOG.fatal(errmsg);
            throw new DbException(errmsg);
        } else {
            LOG.info("Use a TokyoCabinet B+tree index on " + filePath + " for LocalDirectory");
        }
        return tcb;
    }
}
