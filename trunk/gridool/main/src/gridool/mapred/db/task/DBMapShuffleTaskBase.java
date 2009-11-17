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
import xbird.util.concurrent.ExecutorFactory;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public abstract class DBMapShuffleTaskBase<OUT_TYPE> extends GridTaskAdapter {
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

    private transient final ExecutorService shuffleExecPool;
    private transient BoundedArrayQueue<OUT_TYPE> shuffleSink;

    @SuppressWarnings("unchecked")
    public DBMapShuffleTaskBase(GridJob job, @Nonnull DBMapReduceJobConf jobConf) {
        super(job, true);
        assert (jobConf != null);
        this.jobConf = jobConf;
        this.shuffleExecPool = ExecutorFactory.newFixedThreadPool(shuffleThreads(), "Gridool#Shuffle", true);
        this.shuffleSink = new BoundedArrayQueue<OUT_TYPE>(shuffleUnits());
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

    public final Serializable execute() throws GridException {
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
        final DBRecord record = jobConf.createMapInputRecord();
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

    protected final void shuffle(@Nonnull final OUT_TYPE record) {
        assert (shuffleSink != null);
        if(!shuffleSink.offer(record)) {
            invokeShuffle(shuffleExecPool, shuffleSink);
            this.shuffleSink = new BoundedArrayQueue<OUT_TYPE>(shuffleUnits());
            shuffleSink.offer(record);
        }
    }

    protected abstract void invokeShuffle(@Nonnull final ExecutorService shuffleExecPool, @Nonnull final ArrayQueue<OUT_TYPE> queue);

}
