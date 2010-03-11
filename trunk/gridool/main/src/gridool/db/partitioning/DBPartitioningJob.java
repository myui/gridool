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
import gridool.db.partitioning.csv.CsvPartitioningTask;
import gridool.routing.GridTaskRouter;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.util.datetime.StopWatch;
import xbird.util.math.MathUtils;
import xbird.util.primitive.MutableInt;

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

    public Map<GridTask, GridNode> map(GridTaskRouter router, DBPartitioningJobConf jobConf)
            throws GridException {
        this.started = System.currentTimeMillis();
        Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(1);
        CsvPartitioningTask dbtask = jobConf.makePartitioningTask(this);
        GridNode localNode = getJobNode();
        map.put(dbtask, localNode);
        return map;
    }

    @Override
    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        final HashMap<GridNode, MutableInt> processed = result.getResult();
        if(processed == null || processed.isEmpty()) {
            Exception err = result.getException();
            if(err == null) {
                throw new GridException("faileed to execute a task: " + result.getTaskId());
            } else {
                throw new GridException("faileed to execute a task: " + result.getTaskId(), err);
            }
        }
        if(LOG.isInfoEnabled()) {
            final long elapsed = System.currentTimeMillis() - started;
            final int numNodes = processed.size();
            final int[] counts = new int[numNodes];
            int i = 0;
            for(MutableInt e : processed.values()) {
                int v = e.intValue();
                numProcessed += v;
                counts[i++] = v;
            }
            float mean = numProcessed / numNodes;
            float sd = MathUtils.stddev(counts);
            float percent = (sd / mean) * 100.0f;
            LOG.info("STDDEV of data distribution in " + numNodes + " nodes: " + sd + " ("
                    + percent + "%), Job executed in " + StopWatch.elapsedTime(elapsed));
        } else {
            for(MutableInt e : processed.values()) {
                numProcessed += e.intValue();
            }
        }
        return GridTaskResultPolicy.CONTINUE;
    }

    public Long reduce() throws GridException {
        return numProcessed;
    }

}
