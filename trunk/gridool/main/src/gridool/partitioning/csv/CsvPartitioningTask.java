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
package gridool.partitioning.csv;

import gridool.GridException;
import gridool.GridJob;
import gridool.GridKernel;
import gridool.GridNode;
import gridool.annotation.GridKernelResource;
import gridool.construct.GridTaskAdapter;
import gridool.partitioning.PartitioningJobConf;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.util.collections.ArrayQueue;
import xbird.util.collections.BoundedArrayQueue;
import xbird.util.concurrent.DirectExecutorService;
import xbird.util.concurrent.ExecutorFactory;
import xbird.util.concurrent.ExecutorUtils;
import xbird.util.concurrent.collections.ConcurrentIdentityHashMap;
import xbird.util.csv.AdvCvsReader;
import xbird.util.io.FastBufferedInputStream;
import xbird.util.primitive.MutableInt;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public class CsvPartitioningTask extends GridTaskAdapter {
    private static final long serialVersionUID = -4477383489963213348L;
    private static final Log LOG = LogFactory.getLog(CsvPartitioningTask.class);

    @Nonnull
    protected final PartitioningJobConf jobConf;

    // ------------------------
    // injected resources

    @GridKernelResource
    protected transient GridKernel kernel;

    // ------------------------

    private transient final ConcurrentIdentityHashMap<GridNode, MutableInt> assignMap;

    // ------------------------
    // working resources

    protected transient int shuffleUnits = 512;
    protected transient int shuffleThreads = Runtime.getRuntime().availableProcessors();
    protected transient ExecutorService shuffleExecPool;
    protected transient BoundedArrayQueue<String> shuffleSink;

    @SuppressWarnings("unchecked")
    public CsvPartitioningTask(GridJob job, PartitioningJobConf jobConf) {
        super(job, false);
        this.jobConf = jobConf;
        this.assignMap = new ConcurrentIdentityHashMap<GridNode, MutableInt>(64);
    }

    @Override
    public boolean injectResources() {
        return true;
    }

    protected int shuffleUnits() {
        return shuffleUnits;
    }

    public void setShuffleUnits(int shuffleUnits) {
        this.shuffleUnits = shuffleUnits;
    }

    protected int shuffleThreads() {
        return shuffleThreads;
    }

    public void setShuffleThreads(int shuffleThreads) {
        this.shuffleThreads = shuffleThreads;
    }

    public ConcurrentIdentityHashMap<GridNode, MutableInt> execute() throws GridException {
        int numShuffleThreads = shuffleThreads();
        this.shuffleExecPool = (numShuffleThreads <= 0) ? new DirectExecutorService()
                : ExecutorFactory.newFixedThreadPool(numShuffleThreads, "Gridool#Shuffle", true);
        this.shuffleSink = new BoundedArrayQueue<String>(shuffleUnits());

        final AdvCvsReader reader = getCsvReader(jobConf);
        try {
            String line;
            while((line = reader.getNextLine()) != null) {
                shuffle(line);
            }
        } catch (IOException e) {
            LOG.error(e);
            throw new GridException(e);
        }
        postShuffle();
        return assignMap;
    }

    protected void shuffle(@Nonnull final String record) {
        assert (shuffleSink != null);
        if(!shuffleSink.offer(record)) {
            invokeShuffle(shuffleExecPool, shuffleSink);
            this.shuffleSink = new BoundedArrayQueue<String>(shuffleUnits());
            shuffleSink.offer(record);
        }
    }

    protected void postShuffle() {
        if(!shuffleSink.isEmpty()) {
            invokeShuffle(shuffleExecPool, shuffleSink);
        }
        ExecutorUtils.shutdownAndAwaitTermination(shuffleExecPool);
    }

    protected void invokeShuffle(@Nonnull final ExecutorService shuffleExecPool, @Nonnull final ArrayQueue<String> queue) {
        assert (kernel != null);
        shuffleExecPool.execute(new Runnable() {
            public void run() {
                String[] lines = queue.toArray(String.class);
                
            }
        });
    }

    private static final AdvCvsReader getCsvReader(final PartitioningJobConf jobConf)
            throws GridException {
        final String csvPath = jobConf.getCsvFilePath();
        final Reader reader;
        try {
            FileInputStream fis = new FileInputStream(csvPath);
            FastBufferedInputStream bis = new FastBufferedInputStream(fis, 32768);
            reader = new InputStreamReader(bis, "UTF-8");
        } catch (FileNotFoundException fne) {
            LOG.error(fne);
            throw new GridException("CSV file not found: " + csvPath, fne);
        } catch (UnsupportedEncodingException uee) {
            LOG.error(uee);
            throw new IllegalStateException(uee); // should never happens
        }

        final int[] keyIdxs = jobConf.partitionigKeyIndices();
        int maxKeyIdx = -1;
        for(int k : keyIdxs) {
            maxKeyIdx = Math.max(maxKeyIdx, k);
        }
        if(maxKeyIdx < 0) {
            throw new IllegalStateException("Paritioning keys are invalid: "
                    + Arrays.toString(keyIdxs));
        }

        PushbackReader pushback = new PushbackReader(reader);
        return new AdvCvsReader(pushback, keyIdxs, jobConf.getFieldSeparator(), jobConf.getStringQuote());
    }

}
