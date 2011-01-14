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
package gridool.mapred.dht.task;

import gridool.GridException;
import gridool.GridJob;
import gridool.annotation.GridDirectoryResource;
import gridool.construct.GridTaskAdapter;
import gridool.directory.ILocalDirectory;
import gridool.directory.btree.BasicIndexQuery;
import gridool.directory.btree.IndexException;
import gridool.directory.btree.IndexQuery;
import gridool.directory.btree.Value;
import gridool.directory.helpers.FlushableBTreeCallback;
import gridool.mapred.dht.DhtMapReduceJobConf;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public abstract class DhtReduceTask extends GridTaskAdapter {
    private static final long serialVersionUID = 467335434557842030L;
    protected static final Log LOG = LogFactory.getLog(DhtReduceTask.class);

    protected final String inputTableName;
    protected final String destTableName;
    private final boolean removeInputDhtOnFinish;

    @Nullable
    protected DhtMapReduceJobConf jobConf = null;

    // ------------------------
    // injected resources

    @GridDirectoryResource
    protected transient ILocalDirectory directory;

    // ------------------------

    @SuppressWarnings("unchecked")
    public DhtReduceTask(@Nonnull GridJob job, @Nonnull String inputDhtName, @Nonnull String destDhtName, boolean removeInputDhtOnFinish) {
        super(job, true);
        this.inputTableName = inputDhtName;
        this.destTableName = destDhtName;
        this.removeInputDhtOnFinish = removeInputDhtOnFinish;
    }

    @Override
    public final boolean injectResources() {
        return true;
    }

    protected boolean collectOutputKeys() {
        return false;
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public <T extends DhtMapReduceJobConf> T getJobConf() {
        return (T) jobConf;
    }

    public void setJobConf(DhtMapReduceJobConf jobConf) {
        this.jobConf = jobConf;
    }

    /**
     * Override to use a higher selectivity filter.
     * 
     * @see BasicIndexQuery
     */
    protected IndexQuery getQuery() {
        return new BasicIndexQuery.IndexConditionANY();
    }

    /**
     * Override this method to filter key/value pairs.
     * 
     * @return return true to avoid processing this key/value pair.
     */
    protected boolean filter(@Nonnull byte[] key, @Nonnull byte[] value) {
        return false;
    }

    /**
     * Output is collected by calling {@link #collectOutput(byte[], byte[])}.
     * 
     * @return true/false to continue/stop reducing.
     */
    protected boolean process(@Nonnull byte[] key, @Nonnull Collection<byte[]> values) {
        return process(key, values.iterator());
    }

    /**
     * @see #process(byte[], Collection)
     */
    protected abstract boolean process(@Nonnull byte[] key, @Nonnull Iterator<byte[]> values);

    protected abstract void collectOutput(@Nonnull byte[] key, @Nonnull byte[] value);

    protected void postReduce() {}

    protected Serializable execute() throws GridException {
        final FlushableBTreeCallback handler = getHandler();
        try {
            directory.retrieve(inputTableName, getQuery(), handler);
        } catch (IndexException e) {
            LOG.error(e.getMessage(), e);
            throw new GridException(e);
        }
        handler.flush();
        postReduce();

        if(removeInputDhtOnFinish) {
            try {
                directory.drop(inputTableName);
            } catch (IndexException e) {
                LOG.error(e.getMessage(), e);
                throw new GridException(e);
            }
            LOG.info("drop index " + inputTableName);
        }
        return null;
    }

    protected FlushableBTreeCallback getHandler() {
        return new ReduceHandler(this);
    }

    // -----------------------------------------------
    // BTree callback handlers

    private static final class ReduceHandler implements FlushableBTreeCallback {

        private final DhtReduceTask parent;
        private Value prevKey = null;
        private List<byte[]> values;

        private int counter = 0;

        public ReduceHandler(DhtReduceTask parent) {
            this.parent = parent;
            this.values = new ArrayList<byte[]>(4);
        }

        public boolean indexInfo(Value key, byte[] value) {
            boolean doNext = true;
            if(prevKey != null) {
                if(key == prevKey || key.equals(prevKey)) {
                    values.add(value);
                    return true;
                }
                // different key to the previous key was found.            
                // Then, flush previous entry
                doNext = parent.process(prevKey.getData(), values);
                if((++counter) == 10) {
                    parent.reportProgress(-1f);
                }
                this.values = new ArrayList<byte[]>(4);
            }
            this.prevKey = key;

            final byte[] keyData = key.getData();
            if(!parent.filter(keyData, value)) {
                values.add(value);
            }
            return doNext;
        }

        public boolean indexInfo(Value value, long pointer) {
            throw new IllegalStateException();
        }

        public void flush() {
            if(!values.isEmpty()) {
                parent.process(prevKey.getData(), values);
            }
        }
    }

}
