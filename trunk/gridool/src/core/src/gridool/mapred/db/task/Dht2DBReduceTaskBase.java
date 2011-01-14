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
import gridool.mapred.db.DBMapReduceJobConf;
import gridool.mapred.dht.task.DhtReduceTask;
import gridool.marshaller.GridMarshaller;
import gridool.util.collections.ArrayQueue;
import gridool.util.collections.BoundedArrayQueue;
import gridool.util.concurrent.ExecutorFactory;
import gridool.util.concurrent.ExecutorUtils;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public abstract class Dht2DBReduceTaskBase<IN_TYPE, OUT_TYPE> extends DhtReduceTask {
    private static final long serialVersionUID = -8320924089618476538L;
    protected static final Log LOG = LogFactory.getLog(Dht2DBReduceTaskBase.class);

    @Nonnull
    protected final DBMapReduceJobConf jobConf;

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
     * Override to change the number of shuffle units. 1024 by the default.
     */
    protected int shuffleUnits() {
        return 1024;
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
        this.shuffleExecPool = ExecutorFactory.newFixedThreadPool(shuffleThreads(), "Gridool#Reduce", true);
        this.shuffleSink = new BoundedArrayQueue<OUT_TYPE>(shuffleUnits());

        return super.execute();
    }

    @Override
    protected final boolean process(byte[] key, Iterator<byte[]> values) {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected boolean process(byte[] key, Collection<byte[]> values) {
        GridMarshaller<IN_TYPE> marshaller = jobConf.getMapOutputMarshaller();
        DecodingIterator<IN_TYPE> itor = new DecodingIterator<IN_TYPE>(values.iterator(), marshaller);
        return processRecord(key, itor);
    }

    protected abstract boolean processRecord(byte[] key, Iterator<IN_TYPE> values);

    private static final class DecodingIterator<IN_TYPE> implements Iterator<IN_TYPE> {

        private final Iterator<byte[]> delegate;
        private final GridMarshaller<IN_TYPE> marshaller;

        public DecodingIterator(Iterator<byte[]> itor, GridMarshaller<IN_TYPE> marshaller) {
            this.delegate = itor;
            this.marshaller = marshaller;
        }

        public boolean hasNext() {
            return delegate.hasNext();
        }

        public IN_TYPE next() {
            final byte[] b = delegate.next();
            try {
                return marshaller.<IN_TYPE> unmarshall(b, null);
            } catch (GridException e) {// avoid irregular records
                LOG.warn(e.getMessage());
                return null;
            }
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

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
