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

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tokyocabinet.BDB;
import tokyocabinet.BDBCUR;
import gridool.GridNode;
import gridool.communication.payload.GridNodeInfo;
import gridool.locking.LockManager;
import xbird.storage.DbCollection;
import xbird.storage.DbException;
import xbird.util.datetime.StopWatch;
import xbird.util.string.StringUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class TcbLocalDirectory extends AbstractLocalDirectory {
    private static final Log LOG = LogFactory.getLog(TcbLocalDirectory.class);
    private static final String DIR_IDX_NAME = "gridool.metaidx.tcb";

    private final BDB tcb;

    public TcbLocalDirectory(LockManager lockManger) {
        super(lockManger);
        this.tcb = new BDB();
    }

    public void start() throws DbException {
        DbCollection rootColl = DbCollection.getRootCollection();
        File colDir = rootColl.getDirectory();
        assert (colDir.exists()) : colDir.getAbsoluteFile();
        File idxFile = new File(colDir, DIR_IDX_NAME);
        String filePath = idxFile.getAbsolutePath();

        if(!tcb.open(filePath, (BDB.OWRITER | BDB.OCREAT))) {
            int ecode = tcb.ecode();
            String errmsg = "open error: " + BDB.errmsg(ecode);
            LOG.fatal(errmsg);
            throw new DbException(errmsg);
        } else {
            LOG.info("Use a TokyoCabinet B+tree index on " + filePath + " for LocalDirectory");
        }
    }

    public void close() throws DbException {
        if(!tcb.close()) {
            int ecode = tcb.ecode();
            LOG.error("close error: " + BDB.errmsg(ecode));
        }
    }

    public void addMapping(String[] keys, GridNodeInfo node) throws DbException {
        final byte[] value = node.toBytes();
        for(String key : keys) {
            final byte[] kb = StringUtils.getBytes(key);
            if(!tcb.putdup(kb, value)) {
                int ecode = tcb.ecode();
                String errmsg = "put failed: " + BDB.errmsg(ecode);
                LOG.error(errmsg);
                throw new DbException(errmsg);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public List<GridNode> exactSearch(String key, List<GridNode> exclude) throws DbException {
        final byte[] kb = StringUtils.getBytes(key);
        final List<byte[]> results = tcb.getlist(kb);
        if(results == null) {
            return Collections.emptyList();
        }
        final List<GridNode> nodes = new ArrayList<GridNode>(results.size());
        for(byte[] v : results) {
            final GridNodeInfo node = GridNodeInfo.fromBytes(v);
            if(!exclude.contains(node)) {
                if(!nodes.contains(node)) {
                    nodes.add(node);
                }
            }
        }
        return nodes;
    }

    @SuppressWarnings("unchecked")
    public Map<String, List<GridNode>> prefixSearch(String key, Set<GridNode> includeValues)
            throws DbException {
        final long startTime = System.currentTimeMillis();

        final byte[] prefix = StringUtils.getBytes(key);
        final byte[] rightEdge = calculateRightEdgeValue(prefix);

        final List<byte[]> matchedKeys = tcb.range(prefix, true, rightEdge, false, -1);
        if(matchedKeys.isEmpty()) {
            return Collections.emptyMap();
        }

        final Map<String, List<GridNode>> mapping = new HashMap<String, List<GridNode>>(matchedKeys.size());
        int count = 0;
        for(byte[] k : matchedKeys) {
            final String path = StringUtils.toString(k);
            final List<byte[]> values = tcb.getlist(k);
            for(byte[] v : values) {
                count++;
                final GridNodeInfo node = GridNodeInfo.fromBytes(v);
                if(includeValues.contains(node)) {
                    List<GridNode> nodes = mapping.get(path);
                    if(nodes == null) {
                        nodes = new ArrayList<GridNode>(32);
                        mapping.put(path, nodes);
                    }
                    if(!nodes.contains(node)) {
                        nodes.add(node);
                    }
                }
            }
        }

        if(LOG.isInfoEnabled()) {
            long elapsedTime = System.currentTimeMillis() - startTime;
            LOG.info("Elapsed time for TcbLocalDirectory#prefixSearch with a '" + key + "' [ "
                    + count + " items found. ] is " + StopWatch.elapsedTime(elapsedTime));
        }

        return mapping;
    }

    private static final byte[] calculateRightEdgeValue(byte[] v) {
        final int vlen = v.length;
        final byte[] b = new byte[vlen + 1];
        System.arraycopy(v, 0, b, 0, vlen);
        b[vlen] = Byte.MAX_VALUE;
        return b;
    }

    @SuppressWarnings("unchecked")
    public Map<String, List<GridNode>> prefixSearch2(String key, Set<GridNode> includeValues)
            throws DbException {
        final long startTime = System.currentTimeMillis();

        final byte[] prefix = StringUtils.getBytes(key);
        final List<byte[]> results = tcb.fwmkeys(prefix, 1);
        if(results.isEmpty()) {
            return Collections.emptyMap();
        }

        final Map<String, List<GridNode>> mapping = new HashMap<String, List<GridNode>>(128);

        final BDBCUR cursor = new BDBCUR(tcb);
        final byte[] firstMatchedKey = results.get(0);
        if(!cursor.jump(firstMatchedKey)) {
            throw new DbException("Could not jump. Illegal condition was detected for a key: "
                    + key);
        }
        int count = 0;
        do {
            final byte[] k = cursor.key();
            final byte[] v = cursor.val();
            if(k == null || v == null) {
                if(LOG.isWarnEnabled()) {
                    LOG.warn("Found illegal key '" + k + "' or value '" + v + '\'');
                }
                continue;
            }
            if(!startWith(k, prefix)) {
                break;
            }
            count++;

            final String path = StringUtils.toString(k);
            final GridNodeInfo node = GridNodeInfo.fromBytes(v);

            if(includeValues.contains(node)) {//TODO REVIEWME What's preferable behavior when all mapped node is down?
                List<GridNode> nodes = mapping.get(path);
                if(nodes == null) {
                    nodes = new ArrayList<GridNode>(32);
                    mapping.put(path, nodes);
                }
                if(!nodes.contains(node)) {
                    nodes.add(node);
                }
            }
        } while(cursor.next());

        if(LOG.isInfoEnabled()) {
            long elapsedTime = System.currentTimeMillis() - startTime;
            LOG.info("Elapsed time for TcbLocalDirectory#prefixSearch with a '" + key + "' [ "
                    + count + " items found. ] is " + StopWatch.elapsedTime(elapsedTime));
        }

        return mapping;
    }

    private static boolean startWith(final byte[] test, final byte[] prefix) {
        if(test.length < prefix.length) {
            return false;
        }
        for(int i = prefix.length - 1; i >= 0; i--) {
            if(test[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }
}
