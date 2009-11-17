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
package gridool.mapred.db.task;

import gridool.GridException;
import gridool.GridJob;
import gridool.GridKernel;
import gridool.annotation.GridKernelResource;
import gridool.mapred.db.DBMapReduceJobConf;
import gridool.mapred.dht.task.DhtReduceTask;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import xbird.util.collections.ArrayQueue;
import xbird.util.collections.BoundedArrayQueue;
import xbird.util.concurrent.ExecutorFactory;
import xbird.util.concurrent.ExecutorUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public abstract class Dht2DBReduceTaskBase<OUT_TYPE> extends DhtReduceTask {
    private static final long serialVersionUID = -8320924089618476538L;

    @Nonnull
    protected final DBMapReduceJobConf jobConf;
    
    // ------------------------
    // injected resources

    @GridKernelResource
    protected transient GridKernel kernel;

    // ------------------------
    // working resources

    private transient ExecutorService shuffleExecPool;
    private transient BoundedArrayQueue<OUT_TYPE> shuffleSink;

    // ------------------------

    @SuppressWarnings("unchecked")
    public Dht2DBReduceTaskBase(GridJob job, String inputDhtName, String destDhtName, boolean removeInputDhtOnFinish, @Nonnull DBMapReduceJobConf jobConf) {
        super(job, inputDhtName, destDhtName, removeInputDhtOnFinish);
        this.jobConf = jobConf;
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
    public Serializable execute() throws GridException {
        this.shuffleExecPool = ExecutorFactory.newFixedThreadPool(shuffleThreads(), "Gridool#Reduce", true);
        this.shuffleSink = new BoundedArrayQueue<OUT_TYPE>(shuffleUnits());

        return super.execute();
    }

    @Override
    protected boolean process(byte[] key, Iterator<byte[]> values) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean process(byte[] key, Collection<byte[]> values) {
        return super.process(key, values);
    }

    /**
     * Use {@link #collectOutput(Object)} instead.
     */
    @Override
    protected void collectOutput(byte[] key, byte[] value) {
        throw new UnsupportedOperationException();
    }

    protected final void collectOutput(@Nonnull final OUT_TYPE record) {
        assert (shuffleSink != null);
        if(!shuffleSink.offer(record)) {
            invokeShuffle(shuffleExecPool, shuffleSink);
            this.shuffleSink = new BoundedArrayQueue<OUT_TYPE>(shuffleUnits());
            shuffleSink.offer(record);
        }
    }

    protected abstract void invokeShuffle(@Nonnull final ExecutorService shuffleExecPool, @Nonnull final ArrayQueue<OUT_TYPE> queue);

    @Override
    protected void postReduce() {
        if(!shuffleSink.isEmpty()) {
            invokeShuffle(shuffleExecPool, shuffleSink);
        }
        ExecutorUtils.shutdownAndAwaitTermination(shuffleExecPool);
    }
}
