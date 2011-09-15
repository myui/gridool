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
package gridool.db.partitioning.phihash.csv.grace;

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
import gridool.db.partitioning.phihash.DBPartitioningJobConf;
import gridool.db.partitioning.phihash.PartitioningJobType;
import gridool.db.partitioning.phihash.csv.PartitioningJobConf;
import gridool.util.GridUtils;
import gridool.util.collections.ArrayQueue;
import gridool.util.collections.BoundedArrayQueue;
import gridool.util.collections.FixedArrayList;
import gridool.util.collections.ObservableLRUMap;
import gridool.util.concurrent.DirectExecutorService;
import gridool.util.concurrent.ExecutorFactory;
import gridool.util.concurrent.ExecutorUtils;
import gridool.util.csv.CsvUtils;
import gridool.util.csv.CsvReader;
import gridool.util.csv.SimpleCsvReader;
import gridool.util.datetime.TextLongProgressBar;
import gridool.util.datetime.TextProgressBar;
import gridool.util.hashes.FNVHash;
import gridool.util.hashes.HashUtils;
import gridool.util.io.FastBufferedInputStream;
import gridool.util.io.FastBufferedOutputStream;
import gridool.util.io.FileDeletionThread;
import gridool.util.io.FileUtils;
import gridool.util.io.IOUtils;
import gridool.util.primitive.MutableInt;
import gridool.util.primitive.MutableLong;
import gridool.util.primitive.Primitives;
import gridool.util.string.StringUtils;
import gridool.util.struct.Pair;
import gridool.util.system.SystemUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PushbackReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

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
public class CsvGraceHashPartitioningTask extends GridTaskAdapter {
    private static final long serialVersionUID = -4477383489963213348L;
    private static final Log LOG = LogFactory.getLog(CsvGraceHashPartitioningTask.class);

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

    private int shuffleUnits = DEFAULT_SHUFFLE_UNITS; // line 200 bytes * 100 nodes * 20,000 * 4 threads = 1600MB
    private int shuffleThreads = DEFAULT_SHUFFLE_THREADS;

    // ------------------------
    // injected resources

    @GridKernelResource
    protected transient GridKernel kernel;

    @GridRegistryResource
    private transient GridResourceRegistry registry;

    // ------------------------
    // working resources

    private transient ExecutorService shuffleExecPool;
    private transient BoundedArrayQueue<String> shuffleSink;

    protected transient String csvFileName;
    private transient boolean isFirstShuffle = true;
    private transient Pair<PrimaryKey, Collection<ForeignKey>> primaryForeignKeys;

    protected transient HashMap<GridNode, MutableLong> assignMap;
    @Nullable
    private transient Map<String, OutputStream> outputMap;

    @SuppressWarnings("unchecked")
    public CsvGraceHashPartitioningTask(GridJob job, DBPartitioningJobConf jobConf) {
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

    @Override
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

        ObservableLRUMap.Cleaner<String, OutputStream> cleaner = new ObservableLRUMap.Cleaner<String, OutputStream>() {
            public void cleanup(String key, OutputStream out) {
                try {
                    out.flush();
                } catch (IOException e) {
                    throw new IllegalStateException("failed to flush: " + key, e);
                }
                IOUtils.closeQuietly(out);
            }
        };
        // could not allocate large cache because GzipOutputStream consumes non-heap memory
        final Map<String, OutputStream> outputMap = new ObservableLRUMap<String, OutputStream>(128, cleaner);
        this.outputMap = outputMap;

        final int numShuffled;
        if(GridDbUtils.hasParentTable(primaryForeignKeys.getFirst())) {
            numShuffled = twoPassParseAndShuffle();
        } else {
            numShuffled = onePassParseAndShuffle();
        }
        postShuffle(numShuffled);

        for(OutputStream os : outputMap.values()) {
            try {
                os.flush();
            } catch (IOException ioe) {
                throw new GridException(ioe);
            }
            IOUtils.closeQuietly(os);
        }
        this.outputMap = null;

        return assignMap;
    }

    private String generateCsvFileName() {
        String tblName = jobConf.getTableName();
        GridNode senderNode = getSenderNode();
        String addr = senderNode.getPhysicalAdress().getHostAddress();
        return tblName + addr + ".csv";
    }

    private int onePassParseAndShuffle() throws GridException {
        String csvfile = jobConf.getCsvFilePath();
        char filedSeparator = jobConf.getFieldSeparator();
        char quoteChar = jobConf.getStringQuote();

        final long filesize = FileUtils.getFileSize(new File(csvfile));
        final PartitioningLongProgressBar progress = new PartitioningLongProgressBar("[Partitioning] progress of CSV parsing and shuffling", filesize);

        final CsvReader reader = getCsvReader(csvfile, filedSeparator, quoteChar);
        int numShuffled = 0;
        try {
            String line;
            while((line = reader.getNextLine()) != null) {
                shuffle(line);
                numShuffled++;
                long bytes = line.length() + 1;
                progress.inc(bytes);
            }
        } catch (IOException e) {
            LOG.error(e);
            throw new GridException(e);
        } finally {
            IOUtils.closeQuietly(reader);
        }
        progress.finish();
        return numShuffled;
    }

    private int twoPassParseAndShuffle() throws GridException {
        final int numBuckets = jobConf.getNumberOfBuckets();
        if(numBuckets <= 0) {
            throw new GridException("Illegal number of buckets for grace hash partitioning: "
                    + numBuckets);
        }
        if(!HashUtils.isPowerOfTwo(numBuckets)) {
            throw new GridException("number of buckets is not power of two: " + numBuckets);
        }
        // #1 divide the input CSV file
        int totalShuffled = divideInputCsvFile(primaryForeignKeys, numBuckets);

        String csvfile = jobConf.getCsvFilePath();
        final String csvFileName = FileUtils.basename(csvfile);
        final char filedSeparator = jobConf.getFieldSeparator();
        final char quoteChar = jobConf.getStringQuote();

        // #2 run partitioning
        final PartitioningProgressBar progress = new PartitioningProgressBar("[Partitioning] #2nd phase. Progress of CSV parsing and shuffling", totalShuffled);
        int numShuffled = 0;
        for(int bucketNo = 0; bucketNo < numBuckets; bucketNo++) {//for each bucket           
            File workdir = GridUtils.getWorkDir(false);
            File chunkfile = new File(workdir, csvFileName + "." + bucketNo);
            if(!chunkfile.exists()) {
                if(LOG.isDebugEnabled()) {
                    LOG.debug("CSV chunk file is not found: " + chunkfile.getAbsolutePath());
                }
                continue;
            }
            final CsvReader reader = getCsvReader(chunkfile.getAbsolutePath(), filedSeparator, quoteChar);
            try {
                String line;
                while((line = reader.getNextLine()) != null) {
                    shuffle(line, bucketNo);
                    numShuffled++;
                    progress.inc();
                }
            } catch (IOException e) {
                LOG.error(e);
                throw new GridException(e);
            } finally {
                IOUtils.closeQuietly(reader);
                new FileDeletionThread(chunkfile, LOG).start();
            }
            forceShuffle(bucketNo);
        }
        progress.finish();
        return numShuffled;
    }

    private int divideInputCsvFile(final Pair<PrimaryKey, Collection<ForeignKey>> primaryForeignKeys, final int numBuckets)
            throws GridException {
        PrimaryKey primaryKey = primaryForeignKeys.getFirst();
        final int[] pkeyIndicies = primaryKey.getColumnPositions(true);
        assert (pkeyIndicies.length > 0);
        final String[] fields = new String[pkeyIndicies.length];
        final FixedArrayList<String> fieldList = new FixedArrayList<String>(fields);

        // COPY INTO control resources 
        final char filedSeparator = jobConf.getFieldSeparator();
        final char quoteChar = jobConf.getStringQuote();
        final StringBuilder strBuf = new StringBuilder(64);
        final Charset charset = Charset.forName("UTF-8");

        final Map<String, OutputStream> outputMap = new HashMap<String, OutputStream>(numBuckets * 10);
        String csvfile = jobConf.getCsvFilePath();
        final String csvFileName = FileUtils.basename(csvfile);

        final long filesize = FileUtils.getFileSize(new File(csvfile));
        final PartitioningLongProgressBar progress = new PartitioningLongProgressBar("[Partitioning] #1st phase. Progress of CSV division", filesize);

        final int bucketShift = HashUtils.shiftsForNextPowerOfTwo(numBuckets);
        final CsvReader reader = getCsvReader(csvfile, filedSeparator, quoteChar);
        int numShuffled = 0;
        try {
            String line;
            while((line = reader.getNextLine()) != null) {
                CsvUtils.retrieveFields(line, pkeyIndicies, fieldList, filedSeparator, quoteChar);
                fieldList.trimToZero();
                String pkeysField = GridDbUtils.combineFields(fields, pkeyIndicies.length, strBuf);
                // "primary" fragment mapping
                byte[] distkey = StringUtils.getBytes(pkeysField);
                int hashcode = FNVHash.hash32(distkey);
                int bucket = HashUtils.positiveXorFolding(hashcode, bucketShift);
                OutputStream out = prepareOutput(csvFileName, bucket, outputMap);
                byte[] lineBytes = line.getBytes(charset);
                out.write(lineBytes);
                out.write('\n');
                progress.inc(lineBytes.length + 1);
                numShuffled++;
            }
        } catch (IOException e) {
            LOG.error(e);
            throw new GridException(e);
        } finally {
            IOUtils.closeQuietly(reader);
            for(OutputStream os : outputMap.values()) {
                try {
                    os.flush();
                } catch (IOException ioe) {
                    throw new GridException(ioe);
                }
                IOUtils.closeQuietly(os);
            }
        }
        progress.finish();
        return numShuffled;
    }

    private static OutputStream prepareOutput(final String csvFileName, final int bucket, final Map<String, OutputStream> outputMap) {
        String fname = csvFileName + "." + bucket;
        OutputStream out = outputMap.get(fname);
        if(out == null) {
            File dir = GridUtils.getWorkDir(false);
            File file = new File(dir, fname);
            final FileOutputStream fos;
            try {
                fos = new FileOutputStream(file, false);
            } catch (FileNotFoundException fe) {
                throw new IllegalStateException("Failed to create FileOutputStream: "
                        + file.getAbsolutePath(), fe);
            }
            out = new FastBufferedOutputStream(fos, 16384);
            outputMap.put(fname, out);
        }
        return out;
    }

    private void shuffle(@Nonnull final String record) {
        shuffle(record, -1);
    }

    private void shuffle(@Nonnull final String record, final int bucket) {
        assert (shuffleSink != null);
        if(!shuffleSink.offer(record)) {
            invokeShuffle(shuffleExecPool, shuffleSink, bucket);
            this.shuffleSink = new BoundedArrayQueue<String>(shuffleUnits());
            shuffleSink.offer(record);
        }
    }

    private void forceShuffle(final int bucket) {
        if(!shuffleSink.isEmpty()) {
            invokeShuffle(shuffleExecPool, shuffleSink, bucket);
            this.shuffleSink = new BoundedArrayQueue<String>(shuffleUnits());
        }
    }

    protected void postShuffle(final int numShuffled) {
        if(!shuffleSink.isEmpty()) {
            invokeShuffle(shuffleExecPool, shuffleSink, -1);
        }
        ExecutorUtils.shutdownAndAwaitTermination(shuffleExecPool);
    }

    private final void invokeShuffle(@Nonnull final ExecutorService shuffleExecPool, @Nonnull final ArrayQueue<String> queue, final int bucket) {
        assert (kernel != null);
        final String[] lines = queue.toArray(String.class);
        final String fileName = csvFileName;
        if(isFirstShuffle) {
            PartitioningJobConf conf = new PartitioningJobConf(lines, fileName, true, primaryForeignKeys, jobConf, bucket);
            runShuffleJob(kernel, conf, assignMap, outputMap, deploymentGroup);
            this.isFirstShuffle = false;
        } else {
            shuffleExecPool.execute(new Runnable() {
                public void run() {
                    PartitioningJobConf conf = new PartitioningJobConf(lines, fileName, false, primaryForeignKeys, jobConf, bucket);
                    runShuffleJob(kernel, conf, assignMap, outputMap, deploymentGroup);
                }
            });
        }
    }

    private static void runShuffleJob(final GridKernel kernel, final PartitioningJobConf conf, final Map<GridNode, MutableLong> recMap, final Map<String, OutputStream> outputMap, final String deploymentGroup) {
        if(outputMap != null) {
            conf.setOutputMap(outputMap);
        }
        PartitioningJobType jobType = conf.getJobConf().getJobType();
        Class<? extends GridJob<PartitioningJobConf, Map<GridNode, MutableInt>>> jobClass = jobType.getFirstPartitioningJobClass();
        //final GridJobFuture<Map<GridNode, MutableInt>> future = kernel.execute(CsvGraceHashPartitioningJob.class, conf);
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

    private static final CsvReader getCsvReader(final String csvPath, final char filedSeparator, final char quoteChar)
            throws GridException {
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
        return new SimpleCsvReader(pushback, filedSeparator, quoteChar);
    }

    private static final class PartitioningProgressBar extends TextProgressBar {

        PartitioningProgressBar(String title, int totalSteps) {
            super(title, totalSteps);
            setRefreshTime(30000L);
            setRefreshFluctations(10);
        }

        @Override
        protected void show() {
            if(LOG.isInfoEnabled()) {
                LOG.info(getInfo());
            }
        }
    }

    private static final class PartitioningLongProgressBar extends TextLongProgressBar {

        PartitioningLongProgressBar(String title, long totalSteps) {
            super(title, totalSteps);
            setRefreshTime(30000L);
            setRefreshFluctations(10);
        }

        @Override
        protected void show() {
            if(LOG.isInfoEnabled()) {
                LOG.info(getInfo());
            }
        }
    }
}
