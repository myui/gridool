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
import gridool.GridResourceRegistry;
import gridool.annotation.GridKernelResource;
import gridool.annotation.GridRegistryResource;
import gridool.construct.GridTaskAdapter;
import gridool.db.helpers.DBAccessor;
import gridool.db.helpers.ForeignKey;
import gridool.db.helpers.GridDbUtils;
import gridool.db.helpers.PrimaryKey;
import gridool.db.partitioning.DBPartitioningJobConf;
import gridool.directory.ILocalDirectory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.config.Settings;
import xbird.storage.DbException;
import xbird.util.collections.ArrayQueue;
import xbird.util.collections.BoundedArrayQueue;
import xbird.util.concurrent.DirectExecutorService;
import xbird.util.concurrent.ExecutorFactory;
import xbird.util.concurrent.ExecutorUtils;
import xbird.util.csv.CvsReader;
import xbird.util.csv.SimpleCvsReader;
import xbird.util.io.FastBufferedInputStream;
import xbird.util.primitive.MutableInt;
import xbird.util.primitive.Primitives;
import xbird.util.struct.Pair;
import xbird.util.system.SystemUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class CsvPartitioningTask extends GridTaskAdapter {
    private static final long serialVersionUID = -4477383489963213348L;
    private static final Log LOG = LogFactory.getLog(CsvPartitioningTask.class);

    private static final int DEFAULT_SHUFFLE_UNITS;
    private static final int DEFAULT_SHUFFLE_THREADS;
    static {
        DEFAULT_SHUFFLE_UNITS = Primitives.parseInt(Settings.get("gridool.db.partitioning.shuffle_units"), 20000);
        int defaultNumThread = Math.max(2, SystemUtils.availableProcessors() - 1);
        DEFAULT_SHUFFLE_THREADS = Primitives.parseInt(Settings.get("gridool.db.partitioning.shuffle_threads"), defaultNumThread);
    }

    @Nonnull
    protected final DBPartitioningJobConf jobConf;

    // ------------------------
    // injected resources

    @GridKernelResource
    protected transient GridKernel kernel;

    @GridRegistryResource
    private transient GridResourceRegistry registry;

    // ------------------------

    protected transient final ConcurrentHashMap<GridNode, MutableInt> assignMap;

    // ------------------------
    // working resources

    protected transient int shuffleUnits = DEFAULT_SHUFFLE_UNITS; // line 200 bytes * 100 nodes * 20,000 * 4 threads = 1600MB
    protected transient int shuffleThreads = DEFAULT_SHUFFLE_THREADS;

    protected transient ExecutorService shuffleExecPool;
    protected transient BoundedArrayQueue<String> shuffleSink;

    protected transient String csvFileName;
    private transient boolean isFirstShuffle = true;
    private transient Pair<PrimaryKey, Collection<ForeignKey>> primaryForeignKeys;

    @SuppressWarnings("unchecked")
    public CsvPartitioningTask(GridJob job, DBPartitioningJobConf jobConf) {
        super(job, false);
        this.jobConf = jobConf;
        this.assignMap = new ConcurrentHashMap<GridNode, MutableInt>(64);
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

    protected ConcurrentHashMap<GridNode, MutableInt> execute() throws GridException {
        int numShuffleThreads = shuffleThreads();
        this.shuffleExecPool = (numShuffleThreads <= 0) ? new DirectExecutorService()
                : ExecutorFactory.newBoundedWorkQueueFixedThreadPool(numShuffleThreads, "Gridool#Shuffle", true);
        this.shuffleSink = new BoundedArrayQueue<String>(shuffleUnits());
        this.csvFileName = generateCsvFileName();

        // inquire primary foreign keys of the partitioning table
        DBAccessor dba = registry.getDbAccessor();
        String templateTableName = jobConf.getBaseTableName();
        this.primaryForeignKeys = GridDbUtils.getPrimaryForeignKeys(dba, templateTableName);

        // parse and shuffle a CSV file
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

        // clear index buffer
        ILocalDirectory index = registry.getDirectory();
        try {
            index.purgeAll(true);
        } catch (DbException dbe) {
            LOG.error(dbe);
        }
    }

    private final void invokeShuffle(@Nonnull final ExecutorService shuffleExecPool, @Nonnull final ArrayQueue<String> queue) {
        assert (kernel != null);
        final boolean isFirst;
        if(isFirstShuffle) {
            this.isFirstShuffle = false;
            isFirst = true;
        } else {
            isFirst = false;
        }
        final String fileName = csvFileName;
        shuffleExecPool.execute(new Runnable() {
            public void run() {
                String[] lines = queue.toArray(String.class);
                CsvHashPartitioningJob.JobConf conf = new CsvHashPartitioningJob.JobConf(lines, fileName, isFirst, primaryForeignKeys, jobConf);
                final GridJobFuture<Map<GridNode, MutableInt>> future = kernel.execute(CsvHashPartitioningJob.class, conf);
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
                final ConcurrentHashMap<GridNode, MutableInt> recMap = assignMap;
                synchronized(recMap) {
                    for(Map.Entry<GridNode, MutableInt> e : map.entrySet()) {
                        GridNode node = e.getKey();
                        MutableInt assigned = e.getValue();
                        final MutableInt prev = recMap.putIfAbsent(node, assigned);
                        if(prev != null) {
                            int v = assigned.intValue();
                            prev.add(v);
                        }
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
