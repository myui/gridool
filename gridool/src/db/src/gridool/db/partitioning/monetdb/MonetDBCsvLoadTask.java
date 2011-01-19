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
package gridool.db.partitioning.monetdb;

import gridool.GridJob;
import gridool.GridJobFuture;
import gridool.GridNode;
import gridool.db.helpers.GridDbUtils;
import gridool.db.partitioning.DBPartitioningJobConf;
import gridool.db.partitioning.csv.normal.CsvPartitioningTask;
import gridool.util.datetime.StopWatch;
import gridool.util.primitive.MutableLong;
import gridool.util.struct.Pair;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class MonetDBCsvLoadTask extends CsvPartitioningTask {
    private static final long serialVersionUID = -7936659983084252804L;
    private static final Log LOG = LogFactory.getLog(MonetDBCsvLoadTask.class);

    @SuppressWarnings("unchecked")
    public MonetDBCsvLoadTask(GridJob job, DBPartitioningJobConf jobConf) {
        super(job, jobConf);
    }

    @Override
    protected void postShuffle(int numShuffled) {
        super.postShuffle(numShuffled);
        assert (csvFileName != null);

        String connectUrl = jobConf.getConnectUrl();
        String tableName = jobConf.getTableName();
        String createTableDDL = jobConf.getCreateTableDDL();
        String copyIntoQuery = GridDbUtils.generateCopyIntoQuery(tableName, jobConf);
        String alterTableDDL = jobConf.getAlterTableDDL();

        MonetDBCsvLoadOperation ops = new MonetDBCsvLoadOperation(connectUrl, tableName, csvFileName, createTableDDL, copyIntoQuery, alterTableDDL);
        ops.setAuth(jobConf.getUserName(), jobConf.getPassword());
        final Pair<MonetDBCsvLoadOperation, Map<GridNode, MutableLong>> pair = new Pair<MonetDBCsvLoadOperation, Map<GridNode, MutableLong>>(ops, assignMap);

        final StopWatch sw = new StopWatch();
        final GridJobFuture<Long> future = kernel.execute(MonetDBInvokeCsvLoadJob.class, pair);
        final Long numInserted;
        try {
            numInserted = future.get();
        } catch (InterruptedException ie) {
            LOG.error(ie.getMessage(), ie);
            throw new IllegalStateException(ie);
        } catch (ExecutionException ee) {
            LOG.error(ee.getMessage(), ee);
            throw new IllegalStateException(ee);
        }
        assert (numInserted != null);
        if(LOG.isInfoEnabled()) {
            LOG.info("Processed/Inserted " + numShuffled + '/' + numInserted.longValue()
                    + " records into '" + tableName + "' table in " + sw.toString());
        }
    }

}
