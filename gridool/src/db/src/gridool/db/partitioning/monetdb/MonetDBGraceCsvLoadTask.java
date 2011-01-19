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
import gridool.db.helpers.GridDbUtils;
import gridool.db.partitioning.DBPartitioningJobConf;
import gridool.db.partitioning.csv.grace.CsvGraceHashPartitioningTask;
import gridool.util.datetime.StopWatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class MonetDBGraceCsvLoadTask extends CsvGraceHashPartitioningTask {
    private static final long serialVersionUID = -7936659983084252804L;
    private static final Log LOG = LogFactory.getLog(MonetDBGraceCsvLoadTask.class);

    private final boolean invokeCsvLoad;

    @SuppressWarnings("unchecked")
    public MonetDBGraceCsvLoadTask(GridJob job, DBPartitioningJobConf jobConf) {
        this(job, jobConf, true);
    }

    @SuppressWarnings("unchecked")
    public MonetDBGraceCsvLoadTask(GridJob job, DBPartitioningJobConf jobConf, boolean invokeCsvLoad) {
        super(job, jobConf);
        this.invokeCsvLoad = invokeCsvLoad;
    }

    @Override
    protected void postShuffle(int numShuffled) {
        super.postShuffle(numShuffled);

        if(invokeCsvLoad) {
            assert (csvFileName != null);
            StopWatch sw = new StopWatch();
            long numInserted = GridDbUtils.invokeCsvLoadJob(kernel, csvFileName, assignMap, jobConf);
            if(LOG.isInfoEnabled()) {
                LOG.info("Processed/Inserted " + numShuffled + '/' + numInserted
                        + " records into '" + jobConf.getTableName() + "' table in "
                        + sw.toString());
            }
        }
    }
}
