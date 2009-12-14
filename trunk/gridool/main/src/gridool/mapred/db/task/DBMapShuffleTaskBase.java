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
import gridool.construct.GridTaskAdapter;
import gridool.lib.db.DBRecord;
import gridool.mapred.db.DBMapReduceJobConf;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.util.collections.ArrayQueue;
import xbird.util.collections.BoundedArrayQueue;
import xbird.util.concurrent.DirectExecutorService;
import xbird.util.concurrent.ExecutorFactory;
import xbird.util.concurrent.ExecutorUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public abstract class DBMapShuffleTaskBase<IN_TYPE extends DBRecord, OUT_TYPE> extends
        GridTaskAdapter {
    private static final long serialVersionUID = -4028443291695700765L;
    protected static final Log LOG = LogFactory.getLog(DBMapShuffleTaskBase.class);

    @Nonnull
    protected final DBMapReduceJobConf jobConf;

    // ------------------------
    // injected resources

    @GridKernelResource
    protected transient GridKernel kernel;

    // ------------------------
    // working resources

    private transient int shuffleUnits = 512;
    private transient int shuffleThreads = Runtime.getRuntime().availableProcessors();
    private transient ExecutorService shuffleExecPool;
    private transient BoundedArrayQueue<OUT_TYPE> shuffleSink;

    @SuppressWarnings("unchecked")
    public DBMapShuffleTaskBase(GridJob job, @Nonnull DBMapReduceJobConf jobConf) {
        super(job, true);
        assert (jobConf != null);
        this.jobConf = jobConf;
    }

    @Override
    public boolean injectResources() {
        return true;
    }

    /**
     * Override to change the number of shuffle units. 512 by the default.
     */
    protected int shuffleUnits() {
        return shuffleUnits;
    }

    public void setShuffleUnits(int shuffleUnits) {
        this.shuffleUnits = shuffleUnits;
    }

    /**
     * Override to change the number of shuffle threads.
     * Shuffle implies burst network traffic. 
     * 
     * @return number of shuffle threads. {@link Runtime#availableProcessors()} by the default.
     */
    protected int shuffleThreads() {
        return shuffleThreads;
    }

    public void setShuffleThreads(int shuffleThreads) {
        this.shuffleThreads = shuffleThreads;
    }

    public final Serializable execute() throws GridException {
        int numShuffleThreads = shuffleThreads();
        this.shuffleExecPool = (numShuffleThreads <= 0) ? new DirectExecutorService()
                : ExecutorFactory.newFixedThreadPool(numShuffleThreads, "Gridool#Shuffle", true);
        this.shuffleSink = new BoundedArrayQueue<OUT_TYPE>(shuffleUnits());

        // execute a query
        final Connection conn;
        try {
            conn = jobConf.getConnection(false);
            configureConnection(conn);
        } catch (ClassNotFoundException e) {
            LOG.error(e);
            throw new GridException(e);
        } catch (SQLException e) {
            LOG.error(e);
            throw new GridException(e);
        }
        assert (conn != null);
        final String query = jobConf.getInputQuery();
        final Statement statement;
        final ResultSet results;
        try {
            statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            results = statement.executeQuery(query);
        } catch (SQLException e) {
            try {
                conn.close();
            } catch (SQLException sqle) {// avoid
                LOG.debug(sqle.getMessage());
            }
            LOG.error(e);
            throw new GridException(e);
        }

        try {
            preprocess(conn, results);
        } catch (SQLException e) {
            LOG.error(e);
            throw new GridException(e);
        }

        // Iterate over records
        // process -> shuffle is consequently called
        try {
            while(results.next()) {
                IN_TYPE record = prepareInputRecord();
                readFields(record, results);
                if(!process(record)) {
                    break;
                }
            }
        } catch (SQLException e) {
            LOG.error(e);
            throw new GridException(e);
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                LOG.debug("failed closing a statement", e);
            }
            try {
                conn.close();
            } catch (SQLException e) {
                LOG.debug("failed closing a connection", e);
            }
        }
        postShuffle();
        return null;
    }

    private static void configureConnection(final Connection conn) {
        try {
            conn.setReadOnly(true); // should *not* call setReadOnly in a transaction (for MonetDB)
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            LOG.warn("failed to configure a connection", e);
        }
    }

    protected void preprocess(Connection conn, ResultSet results) throws SQLException {}

    protected IN_TYPE prepareInputRecord() {
        return jobConf.createMapInputRecord();
    }

    protected void readFields(IN_TYPE record, ResultSet results) throws SQLException {
        record.readFields(results);
    }

    /**
     * Process a record. This is the map function.
     * 
     * @return true/false to continue/stop mapping.
     */
    protected abstract boolean process(@Nonnull IN_TYPE record);

    protected final void shuffle(@Nonnull final OUT_TYPE record) {
        assert (shuffleSink != null);
        if(!shuffleSink.offer(record)) {
            invokeShuffle(shuffleExecPool, shuffleSink);
            this.shuffleSink = new BoundedArrayQueue<OUT_TYPE>(shuffleUnits());
            shuffleSink.offer(record);
        }
    }

    protected abstract void invokeShuffle(@Nonnull final ExecutorService shuffleExecPool, @Nonnull final ArrayQueue<OUT_TYPE> queue);

    protected void postShuffle() {
        if(!shuffleSink.isEmpty()) {
            invokeShuffle(shuffleExecPool, shuffleSink);
        }
        ExecutorUtils.shutdownAndAwaitTermination(shuffleExecPool);
    }
}
