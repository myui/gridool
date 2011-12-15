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

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.construct.GridJobBase;
import gridool.routing.GridRouter;
import gridool.sqlet.catalog.MapReduceConf.Reducer;
import gridool.sqlet.catalog.PartitioningConf.Partition;
import gridool.sqlet.mapred.MapShuffleSQLTask.MapShuffleSQLTaskResult;
import gridool.util.GridUtils;
import gridool.util.collections.KeyValueMap;
import gridool.util.lang.PrintUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Makoto YUI
 */
public final class MapShuffleSQLJob extends GridJobBase<MapShuffleSQLJob.JobConf, String> {
    private static final long serialVersionUID = -7609125770016590671L;
    private static final Log LOG = LogFactory.getLog(MapShuffleSQLJob.class);

    private transient Map<String, MapShuffleSQLTask> remainingTasks;
    private transient Map<Partition, Reducer> shuffleMap;

    public MapShuffleSQLJob() {
        super();
    }

    @Override
    public Map<GridTask, GridNode> map(GridRouter router, MapShuffleSQLJob.JobConf jobConf)
            throws GridException {
        checkConfiguration(jobConf);

        Iterator<Reducer> reducerItor = jobConf.reducers.iterator();

        final Map<GridTask, GridNode> taskMap = new KeyValueMap<GridTask, GridNode>();
        final Map<String, MapShuffleSQLTask> reverseMap = new HashMap<String, MapShuffleSQLTask>(jobConf.partitions.size());
        final Map<Partition, Reducer> shuffleMap = new HashMap<Partition, Reducer>();
        for(Partition partition : jobConf.partitions) {
            if(!reducerItor.hasNext()) {
                reducerItor = jobConf.reducers.iterator();
            }
            Reducer reducer = reducerItor.next();

            MapShuffleSQLTask task = new MapShuffleSQLTask(this, partition, reducer, jobConf);
            GridNode node = partition.getNode();
            taskMap.put(task, node);

            String taskId = task.getTaskId();
            reverseMap.put(taskId, task);
        }

        this.remainingTasks = reverseMap;
        this.shuffleMap = shuffleMap;
        return taskMap;
    }

    private static void checkConfiguration(MapShuffleSQLJob.JobConf jobConf) throws GridException {
        if(jobConf.partitions.isEmpty()) {
            throw new GridException("no partitions is specified");
        }
        if(jobConf.reducers.isEmpty()) {
            throw new GridException("no reducer is specified");
        }
    }

    @Override
    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        String taskId = result.getTaskId();
        MapShuffleSQLTask task = remainingTasks.get(taskId);
        if(task == null) {// task already returned by an other node.
            return GridTaskResultPolicy.SKIP;
        }

        MapShuffleSQLTaskResult taskResult = result.getResult();
        if(taskResult == null) {// on task failure                  
            GridNode failedNode = result.getExecutedNode();
            GridException err = result.getException();

            String errmsg = "task '" + result.getTaskId() + "' failed on node '" + failedNode
                    + ":'\n" + PrintUtils.prettyPrintStackTrace(err);
            LOG.warn(errmsg);

        }
    }

    @Override
    public String reduce() throws GridException {
        return null;
    }

    public static final class JobConf implements Serializable {
        private static final long serialVersionUID = 2210060957788316914L;

        @Nonnull
        private final String mapSelectQuery;
        @Nonnull
        private final List<Partition> partitions;
        @Nonnull
        private final List<Reducer> reducers;
        @Nonnull
        private final String outputTblName;

        public JobConf(@Nonnull String mapSelectQuery, @Nonnull List<Partition> partitions, @Nonnull List<Reducer> reducers, @Nullable String outputTblName) {
            this.mapSelectQuery = mapSelectQuery;
            this.partitions = partitions;
            this.reducers = reducers;
            this.outputTblName = (outputTblName == null) ? GridUtils.generateQueryName()
                    : outputTblName;
        }

        public String getMapSelectQuery() {
            return mapSelectQuery;
        }

        public List<Partition> getPartitions() {
            return partitions;
        }

        public List<Reducer> getReducers() {
            return reducers;
        }

        public String getOutputTblName() {
            return outputTblName;
        }

    }

}
