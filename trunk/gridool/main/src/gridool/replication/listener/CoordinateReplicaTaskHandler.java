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
package gridool.replication.listener;

import gridool.GridException;
import gridool.GridJob;
import gridool.GridJobFuture;
import gridool.GridKernel;
import gridool.GridNode;
import gridool.GridResourceRegistry;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.annotation.GridRegistryResource;
import gridool.construct.GridJobBase;
import gridool.construct.GridTaskAdapter;
import gridool.discovery.DiscoveryEvent;
import gridool.replication.ReplicaCoordinatorListener;
import gridool.replication.ReplicationManager;
import gridool.replication.jobs.CoordinateReplicaJobConf;
import gridool.routing.GridTaskRouter;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.util.jdbc.JDBCUtils;
import xbird.util.lang.ArrayUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class CoordinateReplicaTaskHandler implements ReplicaCoordinatorListener {
    private static final Log LOG = LogFactory.getLog(CoordinateReplicaTaskHandler.class);

    private GridKernel kernel;

    public CoordinateReplicaTaskHandler() {}

    public void setup(GridKernel kernel) {
        this.kernel = kernel;
    }

    public boolean onConfigureReplica(GridNode masterNode, List<GridNode> oldReplicas, List<GridNode> newReplicas, CoordinateReplicaJobConf jobConf) {
        assert (kernel != null);

        final List<GridNode> addedReplicas = new ArrayList<GridNode>();
        for(GridNode newNode : newReplicas) {
            if(!oldReplicas.contains(newNode)) {
                addedReplicas.add(newNode);
            }
        }
        final List<GridNode> removedReplicas;
        if(jobConf.isReorg()) {
            removedReplicas = Collections.emptyList();
        } else {
            removedReplicas = new ArrayList<GridNode>();
            for(GridNode oldNode : oldReplicas) {
                if(!newReplicas.contains(oldNode)) {
                    removedReplicas.add(oldNode);
                }
            }
        }

        if(addedReplicas.isEmpty() && removedReplicas.isEmpty()) {
            return true; // no need to run a ConfigureReplicaJob.
        }

        final GridJobFuture<GridNode[]> future = kernel.execute(ConfigureReplicaJob.class, new JobConf(masterNode, addedReplicas, removedReplicas, jobConf));
        final GridNode[] succeedNodes;
        try {
            succeedNodes = future.get();
        } catch (InterruptedException ie) {
            LOG.error(ie);
            return false;
        } catch (ExecutionException ee) {
            LOG.error(ee);
            return false;
        }

        final Iterator<GridNode> itor = addedReplicas.iterator();
        while(itor.hasNext()) {
            GridNode node = itor.next();
            if(Arrays.binarySearch(succeedNodes, node) < 0) {
                LOG.warn("failed to configure replica: " + node);
                itor.remove();
            }
        }
        return true;
    }

    public void onChannelClosed() {}// NOP

    public void onDiscovery(DiscoveryEvent event, GridNode node) {}// NOP

    public static final class ConfigureReplicaJob extends GridJobBase<JobConf, GridNode[]> {
        private static final long serialVersionUID = -8465966895559944935L;

        private transient final List<GridNode> succeedNodes;

        public ConfigureReplicaJob() {
            super();
            this.succeedNodes = new ArrayList<GridNode>(12);
        }

        public Map<GridTask, GridNode> map(GridTaskRouter router, JobConf jobConf)
                throws GridException {
            final GridNode masterNode = jobConf.getMasterNode();
            final CoordinateReplicaJobConf replicaJobConf = jobConf.getJobConf();

            final List<GridNode> addedReplicas = jobConf.getAddedReplicas();
            final List<GridNode> removedReplicas = jobConf.getRemovedReplicas();
            final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(addedReplicas.size()
                    + removedReplicas.size());
            for(GridNode node : addedReplicas) {
                GridTask task = new ConfigureReplicaTask(this, true, masterNode, replicaJobConf);
                map.put(task, node);
            }
            for(GridNode node : removedReplicas) {
                GridTask task = new ConfigureReplicaTask(this, false, masterNode, replicaJobConf);
                map.put(task, node);
            }
            return map;
        }

        public GridTaskResultPolicy result(GridTaskResult result)
                throws GridException {
            final Boolean succeed = result.getResult();
            if(succeed != null && succeed.booleanValue()) {
                GridNode executedNode = result.getExecutedNode();
                if(executedNode == null) {
                    throw new IllegalStateException("Executed node is not set for a task: "
                            + result.getTaskId());
                }
                succeedNodes.add(executedNode);
            } else {
                LOG.warn("failed configuring a replica on node:" + result.getExecutedNode());
            }
            return GridTaskResultPolicy.CONTINUE;
        }

        public GridNode[] reduce() throws GridException {
            return ArrayUtils.toArray(succeedNodes, GridNode[].class);
        }
    }

    private static final class ConfigureReplicaTask extends GridTaskAdapter {
        private static final long serialVersionUID = -469279769831064424L;

        /**
         * true if add replicas, false if remove replicas.
         */
        private final boolean addReplicas;
        @Nonnull
        private final GridNode masterNode;
        @Nonnull
        private final CoordinateReplicaJobConf jobConf;

        @GridRegistryResource
        private transient GridResourceRegistry registry;

        @SuppressWarnings("unchecked")
        ConfigureReplicaTask(@Nonnull GridJob job, boolean addReplicas, @Nonnull GridNode masterNode, @Nonnull CoordinateReplicaJobConf jobConf) {
            super(job, false);
            this.addReplicas = addReplicas;
            this.masterNode = masterNode;
            this.jobConf = jobConf;
        }

        @Override
        public boolean injectResources() {
            return true;
        }

        public Boolean execute() throws GridException {
            assert (registry != null);

            final Connection conn;
            try {
                conn = JDBCUtils.getConnection(jobConf.getPrimaryDbUrl(), jobConf.getDriverClassName(), jobConf.getUser(), jobConf.getPasswd());
                conn.setAutoCommit(false);
            } catch (ClassNotFoundException e) {
                LOG.error(e);
                return Boolean.FALSE;
            } catch (SQLException sqle) {
                LOG.error(sqle);
                return Boolean.FALSE;
            }

            final ReplicationManager replicationMgr = registry.getReplicationManager();
            final boolean succeed;
            try {
                succeed = replicationMgr.configureReplicaDatabase(conn, masterNode, addReplicas);
            } catch (SQLException e) {
                LOG.error(e);
                return Boolean.FALSE;
            } finally {
                JDBCUtils.closeQuietly(conn);
            }
            return succeed;
        }

    }

    static final class JobConf implements Externalizable {

        @Nonnull
        private GridNode masterNode;
        @Nonnull
        private List<GridNode> addedReplicas;
        @Nonnull
        private List<GridNode> removedReplicas;
        @Nonnull
        private CoordinateReplicaJobConf jobConf;

        public JobConf() {}// for Externalizable

        public JobConf(GridNode masterNode, List<GridNode> addedReplicas, List<GridNode> removedReplicas, CoordinateReplicaJobConf jobConf) {
            this.masterNode = masterNode;
            this.addedReplicas = addedReplicas;
            this.removedReplicas = removedReplicas;
            this.jobConf = jobConf;
        }

        GridNode getMasterNode() {
            return masterNode;
        }

        List<GridNode> getAddedReplicas() {
            return addedReplicas;
        }

        List<GridNode> getRemovedReplicas() {
            return removedReplicas;
        }

        CoordinateReplicaJobConf getJobConf() {
            return jobConf;
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.masterNode = (GridNode) in.readObject();
            final int size1 = in.readInt();
            final List<GridNode> addedReplicas = new ArrayList<GridNode>(size1);
            for(int i = 0; i < size1; i++) {
                GridNode node = (GridNode) in.readObject();
                addedReplicas.add(node);
            }
            this.addedReplicas = addedReplicas;
            final int size2 = in.readInt();
            final List<GridNode> removedReplicas = new ArrayList<GridNode>(size2);
            for(int i = 0; i < size1; i++) {
                GridNode node = (GridNode) in.readObject();
                removedReplicas.add(node);
            }
            this.removedReplicas = removedReplicas;
            this.jobConf = (CoordinateReplicaJobConf) in.readObject();
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(masterNode);
            final int size1 = addedReplicas.size();
            out.writeInt(size1);
            for(int i = 0; i < size1; i++) {
                GridNode node = addedReplicas.get(i);
                out.writeObject(node);
            }
            final int size2 = removedReplicas.size();
            out.writeInt(size2);
            for(int i = 0; i < size2; i++) {
                GridNode node = removedReplicas.get(i);
                out.writeObject(node);
            }
            out.writeObject(jobConf);
        }

    }

}
