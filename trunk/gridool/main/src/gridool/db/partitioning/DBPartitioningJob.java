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
import gridool.routing.GridTaskRouter;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class DBPartitioningJob extends GridJobBase<DBPartitioningJobConf, Long> {
    private static final long serialVersionUID = 7267171601356014026L;

    private transient long numProcessed = -1L;

    public DBPartitioningJob() {}

    public Map<GridTask, GridNode> map(GridTaskRouter router, DBPartitioningJobConf jobConf)
            throws GridException {
        Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(1);
        GridTask dbtask = jobConf.makePartitioningTask(this);
        GridNode localNode = getJobNode();
        map.put(dbtask, localNode);
        return map;
    }

    public GridTaskResultPolicy result(GridTask task, GridTaskResult result) throws GridException {
        Long processed = result.getResult();
        if(processed != null) {
            numProcessed = processed.longValue();
        }
        return GridTaskResultPolicy.CONTINUE;
    }

    public Long reduce() throws GridException {
        return numProcessed;
    }

}
