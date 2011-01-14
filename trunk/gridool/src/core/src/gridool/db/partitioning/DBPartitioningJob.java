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
package gridool.db.partitioning;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.construct.GridJobBase;
import gridool.routing.GridRouter;
import gridool.util.datetime.DateTimeFormatter;
import gridool.util.lang.PrintUtils;
import gridool.util.math.MathUtils;
import gridool.util.primitive.MutableLong;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class DBPartitioningJob extends GridJobBase<DBPartitioningJobConf, Long> {
    private static final long serialVersionUID = 7267171601356014026L;
    private static final Log LOG = LogFactory.getLog(DBPartitioningJob.class);

    private transient long numProcessed = 0L;
    private transient long started;

    public DBPartitioningJob() {}

    @Override
    public boolean handleNodeFailure() {
        return false;
    }

    public Map<GridTask, GridNode> map(GridRouter router, DBPartitioningJobConf jobConf)
            throws GridException {
        this.started = System.currentTimeMillis();
        Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(1);
        GridTask dbtask = jobConf.makePartitioningTask(this);
        GridNode localNode = getJobNode();
        map.put(dbtask, localNode);
        return map;
    }

    @Override
    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        final HashMap<GridNode, MutableLong> processed = result.getResult();
        if(processed == null || processed.isEmpty()) {
            Exception err = result.getException();
            if(err == null) {
                throw new GridException("failed to execute a task: " + result.getTaskId());
            } else {
                throw new GridException("failed to execute a task: " + result.getTaskId(), err);
            }
        }
        if(LOG.isInfoEnabled()) {
            final long elapsed = System.currentTimeMillis() - started;
            final int numNodes = processed.size();
            final long[] counts = new long[numNodes];
            long maxRecords = -1L, minRecords = -1L;
            int i = 0;
            for(final MutableLong e : processed.values()) {
                long v = e.longValue();
                numProcessed += v;
                counts[i++] = v;
                maxRecords = Math.max(v, maxRecords);
                minRecords = (minRecords == -1L) ? v : Math.min(v, minRecords);
            }
            double mean = numProcessed / numNodes;
            double sd = MathUtils.stddev(counts);
            double avg = numProcessed / numNodes;
            float percent = ((float) (sd / mean)) * 100.0f;
            LOG.info("Job executed in " + DateTimeFormatter.formatTime(elapsed)
                    + ".\n\tSTDDEV of data distribution in " + numNodes + " nodes: " + sd + " ("
                    + percent + "%)\n\tAverage records: " + PrintUtils.formatNumber(avg)
                    + " [ total: " + numProcessed + ", max: " + maxRecords + ", min: " + minRecords
                    + " ]");
        } else {
            for(MutableLong e : processed.values()) {
                numProcessed += e.intValue();
            }
        }
        return GridTaskResultPolicy.CONTINUE;
    }

    public Long reduce() throws GridException {
        return numProcessed;
    }

}
