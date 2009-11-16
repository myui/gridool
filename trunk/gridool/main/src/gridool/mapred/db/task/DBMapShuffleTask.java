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
import gridool.GridJobFuture;
import gridool.GridKernel;
import gridool.annotation.GridKernelResource;
import gridool.construct.GridTaskAdapter;
import gridool.directory.job.DirectoryAddRecordJob;
import gridool.lib.db.DBInsertOperation;
import gridool.lib.db.DBInsertRecordJob;
import gridool.mapred.DataSource;
import gridool.mapred.db.DBMapReduceJobConf;
import gridool.mapred.db.DBRecord;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.util.collections.ArrayQueue;
import xbird.util.collections.BoundedArrayQueue;
import xbird.util.concurrent.ExecutorFactory;
import xbird.util.struct.Pair;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public abstract class DBMapShuffleTask extends GridTaskAdapter {
    private static final long serialVersionUID = -4028443291695700765L;
    protected static final Log LOG = LogFactory.getLog(DBMapShuffleTask.class);

    @Nonnull
    protected final DBMapReduceJobConf jobConf;

    // ------------------------
    // injected resources

    @GridKernelResource
    protected transient GridKernel kernel;

    // ------------------------
    // working resources

    private transient BoundedArrayQueue<DBRecord> shuffleSink;
    private transient ExecutorService shuffleExecPool;

    @SuppressWarnings("unchecked")
    public DBMapShuffleTask(GridJob job, @Nonnull DBMapReduceJobConf jobConf) {
        super(job, true);
        assert (jobConf != null);
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

    public Serializable execute() throws GridException {
        this.shuffleSink = new BoundedArrayQueue<DBRecord>(shuffleUnits());
        this.shuffleExecPool = ExecutorFactory.newFixedThreadPool(shuffleThreads(), "Gridool#Shuffle", true);

        // execute a query
        final Connection conn;
        final ResultSet results;
        try {
            conn = jobConf.getConnection();
            results = executeQuery(conn, jobConf);
        } catch (ClassNotFoundException e) {
            throw new GridException(e);
        } catch (SQLException e) {
            throw new GridException(e);
        }

        // Iterate over records
        // process -> shuffle is consequently called
        final DBRecord record = jobConf.createInputRecord();
        try {
            while(results.next()) {
                record.readFields(results);
                if(process(record)) {
                    break;
                }
            }
        } catch (SQLException e) {
            throw new GridException(e);
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                LOG.debug("failed closing a connection", e);
            }
        }

        return null;
    }

    private static final ResultSet executeQuery(final Connection conn, final DBMapReduceJobConf jobConf)
            throws ClassNotFoundException, SQLException {
        Statement statement = conn.createStatement(ResultSet.FETCH_FORWARD, ResultSet.CONCUR_READ_ONLY);
        String query = jobConf.getInputQuery();
        ResultSet results = statement.executeQuery(query);
        statement.close();
        return results;
    }

    /**
     * Process a record. This is the map function.
     * 
    * @return true/false to continue/stop mapping.
    */
    protected abstract boolean process(@Nonnull DBRecord record);

    protected final void shuffle(@Nonnull final DBRecord record) {
        assert (shuffleSink != null);
        if(!shuffleSink.offer(record)) {
            invokeShuffle(shuffleSink);
            this.shuffleSink = new BoundedArrayQueue<DBRecord>(shuffleUnits());
            shuffleSink.offer(record);
        }
    }

    private void invokeShuffle(final ArrayQueue<DBRecord> queue) {
        assert (kernel != null);
        shuffleExecPool.execute(new Runnable() {
            public void run() {
                final GridJobFuture<Serializable> future;
                final DataSource destType = jobConf.getDataSinkForShuffleOutput();
                switch(destType) {
                    case dht: {
                        String mapOutputTableName = jobConf.getMapOutputTableName();
                        Pair<String, ArrayQueue<DBRecord>> ops = new Pair<String, ArrayQueue<DBRecord>>(mapOutputTableName, queue);
                        future = kernel.execute(DirectoryAddRecordJob.class, ops);
                        break;
                    }
                    case rdbms: {
                        String driverClassName = jobConf.getDriverClassName();
                        String connectUrl = jobConf.getConnectUrl();
                        String mapOutputTableName = jobConf.getMapOutputTableName();
                        String[] fieldNames = jobConf.getMapOutputFieldNames();
                        DBRecord[] records = queue.toArray();
                        DBInsertOperation ops = new DBInsertOperation(driverClassName, connectUrl, mapOutputTableName, fieldNames, records);
                        ops.setAuth(jobConf.getUserName(), jobConf.getPassword());
                        future = kernel.execute(DBInsertRecordJob.class, ops);
                        break;
                    }
                    default:
                        throw new UnsupportedOperationException("Destination '" + destType
                                + "' is not supported as a shuffle's data sink");
                }
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
}
