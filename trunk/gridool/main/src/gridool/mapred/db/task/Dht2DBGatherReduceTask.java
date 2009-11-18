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

import gridool.GridJob;
import gridool.lib.db.DBInsertOperation;
import gridool.lib.db.DBRecord;
import gridool.mapred.db.DBMapReduceJobConf;

import java.sql.SQLException;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.LogFactory;

import xbird.util.collections.ArrayQueue;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public abstract class Dht2DBGatherReduceTask<IN_TYPE> extends
        Dht2DBReduceTaskBase<IN_TYPE, DBRecord> {
    private static final long serialVersionUID = 674691526635818940L;

    @SuppressWarnings("unchecked")
    public Dht2DBGatherReduceTask(GridJob job, String inputDhtName, String destDhtName, boolean removeInputDhtOnFinish, DBMapReduceJobConf jobConf) {
        super(job, inputDhtName, destDhtName, removeInputDhtOnFinish, jobConf);
    }
    
    protected String getReduceOutputDestinationDbUrl() {//REVIEWME
        return jobConf.getReduceOutputDestinationDbUrl();
    }

    protected String getDriverClassName() {
        return jobConf.getDriverClassName();
    }

    protected String getPassword() {
        return jobConf.getPassword();
    }

    protected String getUserName() {
        return jobConf.getUserName();
    }

    @Override
    protected void invokeShuffle(ExecutorService shuffleExecPool, ArrayQueue<DBRecord> queue) {
        String driverClassName = getDriverClassName();
        String connectUrl = getReduceOutputDestinationDbUrl();
        String mapOutputTableName = jobConf.getReduceOutputTableName();
        String[] fieldNames = jobConf.getReduceOutputFieldNames();
        DBRecord[] records = queue.toArray(DBRecord.class);
        final DBInsertOperation ops = new DBInsertOperation(driverClassName, connectUrl, mapOutputTableName, fieldNames, records);
        ops.setAuth(getUserName(), getPassword());
        try {
            ops.execute();
        } catch (SQLException e) {
            LogFactory.getLog(getClass()).error(e);
        }
    }

}
