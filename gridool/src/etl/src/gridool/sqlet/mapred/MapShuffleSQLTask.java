/*
 * @(#)$Id$
 *
 * Copyright 2010-2011 Makoto YUI
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
package gridool.sqlet.mapred;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import gridool.GridException;
import gridool.GridJob;
import gridool.GridNode;
import gridool.construct.GridTaskAdapter;
import gridool.routing.GridRouter;
import gridool.sqlet.catalog.MapReduceConf.Reducer;
import gridool.sqlet.catalog.PartitioningConf.Partition;
import gridool.sqlet.mapred.MapShuffleSQLJob.JobConf;

/**
 * @author Makoto YUI
 */
public final class MapShuffleSQLTask extends GridTaskAdapter {
    private static final long serialVersionUID = 1264429783143300602L;

    private final Partition partition;
    private final Reducer reducer;
    private final JobConf jobConf;
    
    public MapShuffleSQLTask(GridJob<?,?> job, Partition partition, Reducer reducer, JobConf jobConf) {
        super(job, true);
        this.partition = partition;
        this.reducer = reducer;
        this.jobConf = jobConf;
    }

    @Override
    public List<GridNode> listFailoverCandidates(GridRouter router) {
        List<Partition> slaves = partition.getSlaves();
        if(slaves.isEmpty()) {
            return Collections.emptyList();
        }
        final List<GridNode> slaveNodes = new ArrayList<GridNode>(slaves.size());
        for(Partition slave: slaves) {
            GridNode node = slave.getNode();
            slaveNodes.add(node);
        }
        return slaveNodes;
    }

    @Override
    protected MapShuffleSQLTaskResult execute() throws GridException {
        return null;
    }

    public static final class MapShuffleSQLTaskResult implements Serializable {
        private static final long serialVersionUID = 5592388152489291000L;
        
    }
    
}
