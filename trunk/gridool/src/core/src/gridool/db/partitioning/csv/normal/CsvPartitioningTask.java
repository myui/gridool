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
package gridool.db.partitioning.csv.normal;

import gridool.GridException;
import gridool.GridJob;
import gridool.GridJobFuture;
import gridool.GridKernel;
import gridool.GridNode;
import gridool.GridResourceRegistry;
import gridool.Settings;
import gridool.annotation.GridKernelResource;
import gridool.annotation.GridRegistryResource;
import gridool.construct.GridTaskAdapter;
import gridool.db.helpers.DBAccessor;
import gridool.db.helpers.ForeignKey;
import gridool.db.helpers.GridDbUtils;
import gridool.db.helpers.PrimaryKey;
import gridool.db.partitioning.DBPartitioningJobConf;
import gridool.db.partitioning.PartitioningJobType;
import gridool.db.partitioning.csv.PartitioningJobConf;
import gridool.util.collections.ArrayQueue;
import gridool.util.collections.BoundedArrayQueue;
import gridool.util.concurrent.DirectExecutorService;
import gridool.util.concurrent.ExecutorFactory;
import gridool.util.concurrent.ExecutorUtils;
import gridool.util.csv.CvsReader;
import gridool.util.csv.SimpleCvsReader;
import gridool.util.io.FastBufferedInputStream;
import gridool.util.io.IOUtils;
import gridool.util.primitive.MutableInt;
import gridool.util.primitive.MutableLong;
import gridool.util.primitive.Primitives;
import gridool.util.struct.Pair;
import gridool.util.system.SystemUtils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

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
public class CsvPartitioningTask extends GridTaskAdapter {
    private static final long serialVersionUID = -4477383489963213348L;
    private static final Log LOG = LogFactory.getLog(CsvPartitioningTask.class);

    private static final int csvInputBufSize;
    private static final int DEFAULT_SHUFFLE_UNITS;
    private static final int DEFAULT_SHUFFLE_THREADS;
    static {
        csvInputBufSize = Primitives.parseInt(Settings.get("gridool.db.partitioning.csv_reader.bufsize"), 32 * 1024);
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
    // working resources

    protected transient int shuffleUnits = DEFAULT_SHUFFLE_UNITS; // line 200 bytes * 100 nodes * 20,000 * 4 threads = 1600MB
    protected transient int shuffleThreads = DEFAULT_SHUFFLE_THREADS;

    protected transient ExecutorService shuffleExecPool;
    protected transient BoundedArrayQueue<String> shuffleSink;

    protected HashMap<GridNode, MutableLong> assignMap;

    protected transient String csvFileName;
    private transient boolean isFirstShuffle = true;
    private transient Pair<PrimaryKey, Collection<ForeignKey>> primaryForeignKeys;

    @SuppressWarnings("unchecked")
    public CsvPartitioningTask(GridJob job, DBPartitioningJobConf jobConf) {
        super(job, false);
        this.jobConf = jobConf;
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

    protected HashMap<GridNode, MutableLong> execute() throws GridException {
        int numShuffleThreads = shuffleThreads();
        this.shuffleExecPool = (numShuffleThreads <= 0) ? new DirectExecutorService()
                : ExecutorFactory.newBoundedWorkQueueFixedThreadPool(numShuffleThreads, "Gridool#Shuffle", true, new ThreadPoolExecutor.CallerRunsPolicy());
        this.shuffleSink = new BoundedArrayQueue<String>(shuffleUnits());
        this.csvFileName = generateCsvFileName();
        this.assignMap = new HashMap<GridNode, MutableLong>(64);

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
        } finally {
            IOUtils.closeQuietly(reader);
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
        final String[] lines = queue.toArray(String.class);
        final String fileName = csvFileName;
        if(isFirstShuffle) {
            PartitioningJobConf conf = new PartitioningJobConf(lines, fileName, true, primaryForeignKeys, jobConf);
            runShuffleJob(kernel, conf, assignMap, deploymentGroup);
            this.isFirstShuffle = false;
        } else {
            shuffleExecPool.execute(new Runnable() {
                public void run() {
                    PartitioningJobConf conf = new PartitioningJobConf(lines, fileName, false, primaryForeignKeys, jobConf);
                    runShuffleJob(kernel, conf, assignMap, deploymentGroup);
                }
            });
        }
    }

    private static void runShuffleJob(final GridKernel kernel, final PartitioningJobConf conf, final Map<GridNode, MutableLong> recMap, final String deploymentGroup) {
        PartitioningJobType jobType = conf.getJobConf().getJobType();
        Class<? extends GridJob<PartitioningJobConf, Map<GridNode, MutableInt>>> jobClass = jobType.getFirstPartitioningJobClass();
        //final GridJobFuture<Map<GridNode, MutableInt>> future = kernel.execute(CsvHashPartitioningJob.class, conf);
        //final GridJobFuture<Map<GridNode, MutableInt>> future = kernel.execute(GlobalCsvHashPartitioningJob.class, conf);
        final GridJobFuture<Map<GridNode, MutableInt>> future = kernel.execute(jobClass, conf);
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
        synchronized(recMap) {
            for(final Map.Entry<GridNode, MutableInt> e : map.entrySet()) {
                GridNode node = e.getKey();
                MutableInt assigned = e.getValue();
                long v = assigned.longValue();
                MutableLong prev = recMap.get(node);
                if(prev == null) {
                    recMap.put(node, new MutableLong(v));
                } else {
                    prev.add(v);
                }
            }
        }
    }

    private static final CvsReader getCsvReader(final DBPartitioningJobConf jobConf)
            throws GridException {
        final String csvPath = jobConf.getCsvFilePath();
        final Reader reader;
        try {
            FileInputStream fis = new FileInputStream(csvPath);
            FastBufferedInputStream bis = new FastBufferedInputStream(fis, csvInputBufSize);
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
