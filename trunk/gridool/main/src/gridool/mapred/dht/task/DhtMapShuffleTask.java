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
import gridool.GridJobFuture;
import gridool.GridKernel;
import gridool.annotation.GridDirectoryResource;
import gridool.annotation.GridKernelResource;
import gridool.construct.GridTaskAdapter;
import gridool.directory.ILocalDirectory;
import gridool.directory.helpers.FlushableBTreeCallback;
import gridool.directory.job.DirectoryAddJob;
import gridool.directory.ops.AddOperation;
import gridool.mapred.dht.DhtMapReduceJobConf;
import gridool.util.GridUtils;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.storage.DbException;
import xbird.storage.index.Value;
import xbird.storage.indexer.BasicIndexQuery;
import xbird.storage.indexer.IndexQuery;
import xbird.util.collections.BoundedArrayQueue;
import xbird.util.concurrent.ExecutorFactory;
import xbird.util.concurrent.ExecutorUtils;
import xbird.util.string.StringUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public abstract class DhtMapShuffleTask extends GridTaskAdapter {
    private static final long serialVersionUID = -5082221855283908817L;
    protected static final Log LOG = LogFactory.getLog(DhtMapShuffleTask.class);

    protected final String inputDhtName;
    protected final String destDhtName;

    private final boolean removeInputDhtOnFinish;

    @Nullable
    private DhtMapReduceJobConf jobConf = null;

    // ------------------------
    // injected resources

    @GridDirectoryResource
    protected transient ILocalDirectory directory;

    @GridKernelResource
    protected transient GridKernel kernel;

    // ------------------------
    // working resources

    private transient BoundedArrayQueue<byte[]> shuffleSink;
    private transient ExecutorService shuffleExecPool;

    // ------------------------

    @SuppressWarnings("unchecked")
    public DhtMapShuffleTask(GridJob job, String inputDhtName, String destDhtName, boolean removeInputDhtOnFinish) {
        super(job, true);
        this.inputDhtName = inputDhtName;
        this.destDhtName = destDhtName;
        this.removeInputDhtOnFinish = removeInputDhtOnFinish;
    }

    @Override
    public final boolean injectResources() {
        return true;
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public <T extends DhtMapReduceJobConf> T getJobConf() {
        return (T) jobConf;
    }

    public void setJobConf(DhtMapReduceJobConf jobConf) {
        this.jobConf = jobConf;
    }

    protected boolean collectOutputKeys() {
        return false;
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
     * Override to change the number of shuffle units. 512 by the default.
     */
    protected int shuffleUnits() {
        return 512;
    }

    /**
     * Override to change the number of shuffle threads.
     * Shuffle implies burst network traffic. 
     * 
     * @return number of shuffle threads. {@link Runtime#availableProcessors()} by the default.
     */
    protected int shuffleThreads() {
        return Runtime.getRuntime().availableProcessors();
    }

    protected FlushableBTreeCallback getHandler() {
        return new MapHandler(this);
    }

    public Serializable execute() throws GridException {
        this.shuffleSink = new BoundedArrayQueue<byte[]>(shuffleUnits() * 2);
        this.shuffleExecPool = ExecutorFactory.newFixedThreadPool(shuffleThreads(), "Gridool#Shuffle", true);

        final IndexQuery query = getQuery();
        final FlushableBTreeCallback handler = getHandler();
        try { // filter -> process -> shuffle is consequently called
            directory.retrieve(inputDhtName, query, handler);
        } catch (DbException e) {
            LOG.error(e.getMessage(), e);
            throw new GridException(e);
        }
        handler.flush();
        postShuffle();

        if(removeInputDhtOnFinish) {
            try {
                directory.drop(inputDhtName);
            } catch (DbException e) {
                LOG.error(e.getMessage(), e);
                throw new GridException(e);
            }
            LOG.info("drop index " + inputDhtName);
        }
        return null;
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
     * Process a key/value pair. This is the map function.
     * {@link #shuffle(byte[], byte[])} is called at this function.
     * 
     * @see DhtMapShuffleTask#shuffle(byte[], byte[])
     * @return true/false to continue/stop mapping.
     */
    protected abstract boolean process(@Nonnull byte[] key, @Nonnull byte[] value);

    protected final void shuffle(@Nonnull byte[] key, @Nonnull byte[] value) {
        assert (shuffleSink != null);
        if(!shuffleSink.offer(key)) {
            invokeShuffle(shuffleSink);
            this.shuffleSink = new BoundedArrayQueue<byte[]>(shuffleUnits() * 2);
            shuffleSink.offer(key);
            shuffleSink.offer(value);
        } else {
            if(!shuffleSink.offer(value)) {
                throw new IllegalStateException();
            }
        }
    }

    private void invokeShuffle(final BoundedArrayQueue<byte[]> queue) {
        assert (kernel != null);
        if(collectOutputKeys()) {
            shuffleAndCollectKeys(queue);
            return;
        }

        final AddOperation ops = new AddOperation(destDhtName);
        ops.setMaxNumReplicas(0);

        final int size = queue.size();
        for(int i = 0; i < size; i += 2) {
            byte[] k = queue.get(i);
            byte[] v = queue.get(i + 1);
            ops.addMapping(k, v);
        }

        shuffleExecPool.execute(new Runnable() {
            public void run() {
                final GridJobFuture<Serializable> future = kernel.execute(DirectoryAddJob.class, ops);
                try {
                    future.get(); // wait for execution
                } catch (InterruptedException ie) {
                    LOG.error(ie.getMessage(), ie);
                } catch (ExecutionException ee) {
                    LOG.error(ee.getMessage(), ee);
                }
            }
        });
    }

    private void shuffleAndCollectKeys(final BoundedArrayQueue<byte[]> queue) {
        if(jobConf == null) {
            LOG.warn("jobConf was not set. Use the default OutputKeyCollectionName for collecting keys: "
                    + DhtMapReduceJobConf.OutputKeyCollectionName);
        }
        final AddOperation ops1;
        final AddOperation ops2;
        {
            // #1. shuffle key/values
            ops1 = new AddOperation(destDhtName);
            ops1.setMaxNumReplicas(0);
            final int size = queue.size();
            final byte[][] shuffledKeys = new byte[size / 2][];
            for(int i = 0, j = 0; i < size; i += 2, j++) {
                byte[] k = queue.get(i);
                shuffledKeys[j] = k;
                byte[] v = queue.get(i + 1);
                ops1.addMapping(k, v);
            }
            // #2. collect keys
            String collectKeyDest = (jobConf == null) ? DhtMapReduceJobConf.OutputKeyCollectionName
                    : jobConf.getOutputKeyCollectionName();
            byte[] key = StringUtils.getBytes(destDhtName);
            byte[] value = GridUtils.compressOutputKeys(shuffledKeys);
            ops2 = new AddOperation(collectKeyDest, key, value);
            ops2.setMaxNumReplicas(0);
        }
        shuffleExecPool.execute(new Runnable() {
            public void run() {
                final GridJobFuture<Serializable> future1 = kernel.execute(DirectoryAddJob.class, ops1);
                final GridJobFuture<Serializable> future2 = kernel.execute(DirectoryAddJob.class, ops2);
                try {// TODO REVIEWME order of waiting                    
                    future2.get(); // wait for execution
                    future1.get(); // wait for execution 
                } catch (InterruptedException ie) {
                    LOG.error(ie.getMessage(), ie);
                } catch (ExecutionException ee) {
                    LOG.error(ee.getMessage(), ee);
                }
            }
        });
    }

    protected void postShuffle() {
        if(!shuffleSink.isEmpty()) {
            invokeShuffle(shuffleSink);
        }
        ExecutorUtils.shutdownAndAwaitTermination(shuffleExecPool);
    }

    private static final class MapHandler implements FlushableBTreeCallback {

        private final DhtMapShuffleTask parent;
        private int counter = 0;

        public MapHandler(DhtMapShuffleTask task) {
            super();
            this.parent = task;
        }

        public boolean indexInfo(final Value key, final byte[] value) {
            final byte[] keyData = key.getData();
            if(!parent.filter(keyData, value)) {
                if(!parent.process(keyData, value)) {
                    parent.reportProgress(-1f);
                    return false;
                }
                if((++counter) == 10) {
                    parent.reportProgress(-1f);
                    counter = 0;
                }
            }
            return true;
        }

        public boolean indexInfo(Value value, long pointer) {
            throw new IllegalStateException();
        }

        public void flush() {}
    }

}
