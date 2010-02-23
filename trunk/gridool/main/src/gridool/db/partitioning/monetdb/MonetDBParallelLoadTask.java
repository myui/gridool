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
import gridool.db.partitioning.DBPartitioningJobConf;
import gridool.db.partitioning.csv.CsvPartitioningTask;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.util.datetime.StopWatch;
import xbird.util.primitive.MutableInt;
import xbird.util.struct.Pair;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class MonetDBParallelLoadTask extends CsvPartitioningTask {
    private static final long serialVersionUID = -7936659983084252804L;
    private static final Log LOG = LogFactory.getLog(MonetDBParallelLoadTask.class);

    @SuppressWarnings("unchecked")
    public MonetDBParallelLoadTask(GridJob job, DBPartitioningJobConf jobConf) {
        super(job, jobConf);
    }

    @Override
    protected void postShuffle(int numShuffled) {
        super.postShuffle(numShuffled);
        assert (csvFileName != null);

        String connectUrl = jobConf.getConnectUrl();
        String tableName = jobConf.getTableName();
        String createTableDDL = jobConf.getCreateTableDDL();
        boolean addHiddenField = hasParentTableExportedKey;
        String copyIntoQuery = generateCopyIntoQuery(tableName, jobConf);
        String alterTableDDL = jobConf.getAlterTableDDL();

        MonetDBParallelLoadOperation ops = new MonetDBParallelLoadOperation(connectUrl, tableName, csvFileName, createTableDDL, addHiddenField, copyIntoQuery, alterTableDDL);
        ops.setAuth(jobConf.getUserName(), jobConf.getPassword());
        final Pair<MonetDBParallelLoadOperation, Map<GridNode, MutableInt>> pair = new Pair<MonetDBParallelLoadOperation, Map<GridNode, MutableInt>>(ops, assignMap);

        final StopWatch sw = new StopWatch();
        final GridJobFuture<Long> future = kernel.execute(MonetDBInvokeParallelLoadJob.class, pair);
        final Long numProcessed;
        try {
            numProcessed = future.get();
        } catch (InterruptedException ie) {
            LOG.error(ie.getMessage(), ie);
            throw new IllegalStateException(ie);
        } catch (ExecutionException ee) {
            LOG.error(ee.getMessage(), ee);
            throw new IllegalStateException(ee);
        }
        assert (numProcessed != null);
        if(LOG.isInfoEnabled()) {
            LOG.info("Processed/Inserted " + numShuffled + '/' + numProcessed.longValue()
                    + " records into '" + tableName + "' table in " + sw.toString());
        }
    }

    private static String generateCopyIntoQuery(final String tableName, final DBPartitioningJobConf jobConf) {
        return "COPY INTO \"" + tableName + "\" FROM '<src>' USING DELIMITERS '"
                + jobConf.getFieldSeparator() + "', '" + jobConf.getRecordSeparator() + "', '"
                + jobConf.getStringQuote() + '\'';
    }

}
