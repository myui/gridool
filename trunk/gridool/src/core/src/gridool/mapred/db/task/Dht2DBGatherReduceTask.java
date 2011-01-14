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
import gridool.db.DBInsertOperation;
import gridool.db.record.DBRecord;
import gridool.mapred.db.DBMapReduceJobConf;
import gridool.util.GridUtils;
import gridool.util.collections.ArrayQueue;

import java.sql.SQLException;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.LogFactory;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public abstract class Dht2DBGatherReduceTask<IN_TYPE> extends
        Dht2DBReduceTaskBase<IN_TYPE, DBRecord> {
    private static final long serialVersionUID = 674691526635818940L;

    @SuppressWarnings("unchecked")
    public Dht2DBGatherReduceTask(GridJob job, String inputDhtName, String destDhtName, boolean removeInputDhtOnFinish, DBMapReduceJobConf jobConf) {
        super(job, inputDhtName, destDhtName, removeInputDhtOnFinish, jobConf);
    }

    protected String getReduceOutputDestinationDbUrl() {//REVIEWME
        return jobConf.getReduceOutputDbUrl();
    }

    protected String getDriverClassName() {
        return jobConf.getDriverClassName();
    }

    protected String getReduceOutputTableName() {
        final String tblName = jobConf.getReduceOutputTableName();
        if(jobConf.getQueryTemplateForCreatingViewComposite() != null) {
            return GridUtils.generateTableName(tblName, this);
        }
        return tblName;
    }

    protected String getPassword() {
        return jobConf.getPassword();
    }

    protected String getUserName() {
        return jobConf.getUserName();
    }

    @Override
    protected void invokeShuffle(final ExecutorService shuffleExecPool, final ArrayQueue<DBRecord> queue) {
        shuffleExecPool.execute(new Runnable() {
            public void run() {
                String driverClassName = getDriverClassName();
                String connectUrl = getReduceOutputDestinationDbUrl();
                String mapOutputTableName = getReduceOutputTableName();
                String[] fieldNames = jobConf.getReduceOutputFieldNames();
                DBRecord[] records = queue.toArray(DBRecord.class);
                final DBInsertOperation ops = new DBInsertOperation(driverClassName, connectUrl, null, mapOutputTableName, fieldNames, records);
                ops.setAuth(getUserName(), getPassword());
                try {
                    ops.execute();
                } catch (SQLException sqle) {
                    LogFactory.getLog(getClass()).error(sqle);
                } catch (GridException ge) {
                    LogFactory.getLog(getClass()).error(ge);
                } catch (Throwable te) {
                    LogFactory.getLog(getClass()).error(te);
                }
            }
        });
    }

}
