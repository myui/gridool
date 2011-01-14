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
import gridool.annotation.GridKernelResource;
import gridool.directory.job.DirectoryAddJob;
import gridool.directory.ops.AddOperation;
import gridool.mapred.dht.DhtMapReduceJobConf;
import gridool.util.GridUtils;
import gridool.util.collections.BoundedArrayQueue;
import gridool.util.concurrent.ExecutorFactory;
import gridool.util.concurrent.ExecutorUtils;
import gridool.util.string.StringUtils;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public abstract class DhtScatterReduceTask extends DhtReduceTask {
    private static final long serialVersionUID = -8356359010558153926L;

    // ------------------------
    // injected resources

    @GridKernelResource
    protected transient GridKernel kernel;

    // ------------------------
    // working resources

    protected transient BoundedArrayQueue<byte[]> shuffleSink;
    protected transient ExecutorService shuffleExecPool;

    // ------------------------

    @SuppressWarnings("unchecked")
    public DhtScatterReduceTask(GridJob job, String inputTableName, String destTableName, boolean removeInputDhtOnFinish) {
        super(job, inputTableName, destTableName, removeInputDhtOnFinish);
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

    @Override
    protected Serializable execute() throws GridException {
        this.shuffleSink = new BoundedArrayQueue<byte[]>(shuffleUnits() * 2);
        this.shuffleExecPool = ExecutorFactory.newFixedThreadPool(shuffleThreads(), "Gridool#Shuffle", true);

        return super.execute();
    }

    @Override
    protected void collectOutput(final byte[] key, final byte[] value) {
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

    protected void invokeShuffle(final BoundedArrayQueue<byte[]> queue) {
        assert (kernel != null);
        if(collectOutputKeys()) {
            shuffleAndCollectKeys(queue);
            return;
        }

        final AddOperation ops = new AddOperation(destTableName);
        ops.setMaxNumReplicas(0); // TODO

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
            ops1 = new AddOperation(destTableName);
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
            String collectKeyDest = jobConf.getOutputKeyCollectionName();
            byte[] key = StringUtils.getBytes(destTableName);
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

    @Override
    protected void postReduce() {
        if(!shuffleSink.isEmpty()) {
            invokeShuffle(shuffleSink);
        }
        ExecutorUtils.shutdownAndAwaitTermination(shuffleExecPool);
    }

}
