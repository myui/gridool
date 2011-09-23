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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gridool.GridNode;
import gridool.communication.payload.GridNodeInfo;
import gridool.locking.LockManager;
import xbird.storage.DbCollection;
import xbird.storage.DbException;
import xbird.storage.index.BIndexFile;
import xbird.storage.index.BTreeCallback;
import xbird.storage.index.Value;
import xbird.storage.indexer.IndexQuery;
import xbird.storage.indexer.BasicIndexQuery.IndexConditionEQ;
import xbird.storage.indexer.BasicIndexQuery.IndexConditionSW;
import xbird.util.datetime.StopWatch;
import xbird.util.string.StringUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class DefaultLocalDirectory extends AbstractLocalDirectory {
    private static final Log LOG = LogFactory.getLog(DefaultLocalDirectory.class);
    private static final String DIR_IDX_NAME = "gridool.metaidx.bfile";

    @Nonnull
    private final BIndexFile btree;

    public DefaultLocalDirectory(@CheckForNull LockManager lockManger) {
        super(lockManger);
        DbCollection rootColl = DbCollection.getRootCollection();
        File colDir = rootColl.getDirectory();
        assert (colDir.exists()) : colDir.getAbsoluteFile();
        File idxFile = new File(colDir, DIR_IDX_NAME);
        this.btree = new BIndexFile(idxFile, true);
    }

    public void start() throws DbException {
        File file = btree.getFile();
        if(file.exists()) {
            if(!file.delete()) {
                throw new DbException(DIR_IDX_NAME + " could not prepared on a file: "
                        + file.getAbsolutePath());
            }
        }
        btree.init(false);
        LOG.info("Use an original B+tree index on " + file.getAbsolutePath()
                + " for LocalDirectory");
    }

    public void close() throws DbException {
        btree.drop(); // TODO REVIEWME
    }

    public void addMapping(String[] keys, GridNodeInfo node) throws DbException {
        final byte[] value = node.toBytes();
        for(String k : keys) {
            btree.putValue(new Value(k), value);
        }
    }

    @Nonnull
    public List<GridNode> exactSearch(String key, List<GridNode> exclude) throws DbException {
        Value k = new Value(key);
        IndexQuery query = new IndexConditionEQ(k);
        ResultValueCollector handler = new ResultValueCollector(exclude);
        btree.search(query, handler);
        return handler.getMatched();
    }

    @Nonnull
    public Map<String, List<GridNode>> prefixSearch(String key, Set<GridNode> includeValues)
            throws DbException {
        long startTime = System.currentTimeMillis();
        Value k = new Value(key);
        IndexQuery query = new IndexConditionSW(k);
        ResultKeyValueCollector handler = new ResultKeyValueCollector(includeValues);
        btree.search(query, handler);
        if(LOG.isInfoEnabled()) {
            long elapsedTime = System.currentTimeMillis() - startTime;
            LOG.info("Elapsed time for DefaultLocalDirectory#prefixSearch for a key '" + key
                    + "': " + StopWatch.elapsedTime(elapsedTime));
        }
        return handler.getMapping();
    }

    private static final class ResultValueCollector implements BTreeCallback {

        private final List<GridNode> exclude;
        private final List<GridNode> nodes = new ArrayList<GridNode>(8);

        public ResultValueCollector(List<GridNode> exclude) {
            this.exclude = exclude;
        }

        public List<GridNode> getMatched() {
            return nodes;
        }

        public boolean indexInfo(Value key, byte[] value) {
            final GridNodeInfo node = GridNodeInfo.fromBytes(value);
            if(!exclude.contains(node)) {
                if(!nodes.contains(node)) {
                    nodes.add(node);
                }
            }
            return true;
        }

        public boolean indexInfo(Value value, long pointer) {
            throw new UnsupportedOperationException();
        }

    }

    private static final class ResultKeyValueCollector implements BTreeCallback {

        final Set<GridNode> includeValues;
        final Map<String, List<GridNode>> mapping = new HashMap<String, List<GridNode>>(128);

        public ResultKeyValueCollector(Set<GridNode> includeValues) {
            if(includeValues == null) {
                throw new IllegalArgumentException();
            }
            this.includeValues = includeValues;
        }

        public Map<String, List<GridNode>> getMapping() {
            return mapping;
        }

        public boolean indexInfo(Value key, byte[] value) {
            byte[] keyData = key.getData();
            String path = StringUtils.toString(keyData);
            GridNodeInfo node = GridNodeInfo.fromBytes(value);

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
            return true;
        }

        public boolean indexInfo(Value value, long pointer) {
            throw new UnsupportedOperationException();
        }

    }

}
