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
package gridool.db.partitioning.csv;

import gridool.GridException;
import gridool.GridJob;
import gridool.GridJobFuture;
import gridool.GridKernel;
import gridool.GridNode;
import gridool.annotation.GridKernelResource;
import gridool.construct.GridTaskAdapter;
import gridool.db.partitioning.DBPartitioningJobConf;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
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
import xbird.util.csv.CvsReader;
import xbird.util.csv.SimpleCvsReader;
import xbird.util.io.FastBufferedInputStream;
import xbird.util.primitive.MutableInt;
import xbird.util.struct.Triple;
import xbird.util.system.SystemUtils;

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
    protected final DBPartitioningJobConf jobConf;

    // ------------------------
    // injected resources

    @GridKernelResource
    protected transient GridKernel kernel;

    // ------------------------

    protected transient final ConcurrentIdentityHashMap<GridNode, MutableInt> assignMap;

    // ------------------------
    // working resources

    protected transient int shuffleUnits = 100000;
    protected transient int shuffleThreads = Math.max(2, SystemUtils.availableProcessors() - 1);
    protected transient ExecutorService shuffleExecPool;
    protected transient BoundedArrayQueue<String> shuffleSink;

    protected transient String csvFileName;

    @SuppressWarnings("unchecked")
    public CsvPartitioningTask(GridJob job, DBPartitioningJobConf jobConf) {
        super(job, false);
        this.jobConf = jobConf;
        this.assignMap = new ConcurrentIdentityHashMap<GridNode, MutableInt>(64);
    }

    @Override
    public boolean injectResources() {
        return true;
    }

    public int shuffleUnits() {
        return shuffleUnits;
    }

    public void setShuffleUnits(int shuffleUnits) {
        this.shuffleUnits = shuffleUnits;
    }

    public int shuffleThreads() {
        return shuffleThreads;
    }

    public void setShuffleThreads(int shuffleThreads) {
        this.shuffleThreads = shuffleThreads;
    }

    public final ConcurrentIdentityHashMap<GridNode, MutableInt> execute() throws GridException {
        int numShuffleThreads = shuffleThreads();
        this.shuffleExecPool = (numShuffleThreads <= 0) ? new DirectExecutorService()
                : ExecutorFactory.newBoundedWorkQueueFixedThreadPool(numShuffleThreads, "Gridool#Shuffle", true);
        this.shuffleSink = new BoundedArrayQueue<String>(shuffleUnits());
        this.csvFileName = generateCsvFileName();

        final CvsReader reader = getCsvReader(jobConf);
        int numShuffled = 0;
        try {
            String line;
            while((line = reader.getNextLine()) != null) {
                shuffle(line);
                numShuffled++;
            }
        } catch (IOException e) {
            LOG.error(e);
            throw new GridException(e);
        }
        postShuffle(numShuffled);
        return assignMap;
    }

    private String generateCsvFileName() {
        assert (kernel != null);
        String tblName = jobConf.getTableName();
        GridNode node = getSenderNode();
        String addr = node.getPhysicalAdress().getHostAddress();
        return tblName + addr + ".csv";
    }

    private void shuffle(@Nonnull final String record) {
        assert (shuffleSink != null);
        if(!shuffleSink.offer(record)) {
            invokeShuffle(shuffleExecPool, shuffleSink);
            this.shuffleSink = new BoundedArrayQueue<String>(shuffleUnits());
            shuffleSink.offer(record);
        }
    }

    protected void postShuffle(int numShuffled) {
        if(!shuffleSink.isEmpty()) {
            invokeShuffle(shuffleExecPool, shuffleSink);
        }
        ExecutorUtils.shutdownAndAwaitTermination(shuffleExecPool);
    }

    private final void invokeShuffle(@Nonnull final ExecutorService shuffleExecPool, @Nonnull final ArrayQueue<String> queue) {
        assert (kernel != null);
        final String fileName = csvFileName;
        shuffleExecPool.execute(new Runnable() {
            public void run() {
                String[] lines = queue.toArray(String.class);
                Triple<String[], String, DBPartitioningJobConf> ops = new Triple<String[], String, DBPartitioningJobConf>(lines, fileName, jobConf);
                final GridJobFuture<Map<GridNode, MutableInt>> future = kernel.execute(CsvHashPartitioningJob.class, ops);
                final Map<GridNode, MutableInt> map;
                try {
                    map = future.get(); // wait for execution
                } catch (InterruptedException ie) {
                    LOG.error(ie.getMessage(), ie);
                    throw new IllegalStateException(ie);
                } catch (ExecutionException ee) {
                    LOG.error(ee.getMessage(), ee);
                    throw new IllegalStateException(ee);
                }
                final ConcurrentIdentityHashMap<GridNode, MutableInt> recMap = assignMap;
                for(Map.Entry<GridNode, MutableInt> e : map.entrySet()) {
                    GridNode node = e.getKey();
                    MutableInt assigned = e.getValue();
                    final MutableInt prev = recMap.putIfAbsent(node, assigned);
                    if(prev != null) {
                        prev.add(assigned.intValue());
                    }
                }
            }
        });
    }

    private static final CvsReader getCsvReader(final DBPartitioningJobConf jobConf)
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
        PushbackReader pushback = new PushbackReader(reader);
        return new SimpleCvsReader(pushback, jobConf.getFieldSeparator(), jobConf.getStringQuote());
    }

}
