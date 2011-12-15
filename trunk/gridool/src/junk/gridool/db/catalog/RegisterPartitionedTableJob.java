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
package gridool.db.catalog;

import gridool.GridConfiguration;
import gridool.GridException;
import gridool.GridJob;
import gridool.GridNode;
import gridool.GridResourceRegistry;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.annotation.GridConfigResource;
import gridool.annotation.GridRegistryResource;
import gridool.construct.GridJobBase;
import gridool.construct.GridTaskAdapter;
import gridool.routing.GridRouter;
import gridool.util.struct.Pair;

import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Map;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class RegisterPartitionedTableJob extends GridJobBase<Pair<String[], String>, int[]> {
    private static final long serialVersionUID = -261878420856912614L;

    @GridConfigResource
    private transient GridConfiguration config;
    @GridRegistryResource
    private transient GridResourceRegistry registry;

    private transient int[] partitionIds;

    public RegisterPartitionedTableJob() {
        super();
    }

    @Override
    public boolean injectResources() {
        return true;
    }

    public Map<GridTask, GridNode> map(GridRouter router, Pair<String[], String> jobArgs)
            throws GridException {
        String[] tableNames = jobArgs.getFirst();
        String templateTableNamePrefix = jobArgs.getSecond();
        final int[] partitionIds = registerTables(tableNames, templateTableNamePrefix, registry);

        final GridNode localNode = config.getLocalNode();
        final GridNode[] nodes = router.getAllNodes();
        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(nodes.length);
        for(final GridNode node : nodes) {
            if(!node.equals(localNode)) {
                GridTask task = new RegisterPartitionedTableTask(this, tableNames, templateTableNamePrefix);
                map.put(task, node);
            }
        }

        this.partitionIds = partitionIds;
        return map;
    }

    @Override
    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        final int[] registeredTids = result.getResult();
        if(registeredTids == null) {
            GridNode node = result.getExecutedNode();
            String errmsg = getClass().getSimpleName() + " failed on node: " + node;
            GridException err = result.getException();
            throw new GridException(errmsg, err);
        } else {
            if(!Arrays.equals(partitionIds, registeredTids)) {
                GridNode node = result.getExecutedNode();
                throw new GridException("Registered partitionIds "
                        + Arrays.toString(registeredTids) + " differ on node: " + node);
            }
        }
        return GridTaskResultPolicy.CONTINUE;
    }

    public int[] reduce() throws GridException {
        return partitionIds;
    }

    private static int[] registerTables(final String[] tableNames, final String templateTableNamePrefix, final GridResourceRegistry registry)
            throws GridException {
        final DistributionCatalog catalog = registry.getDistributionCatalog();
        int[] partitionIds = catalog.bindTableId(tableNames, templateTableNamePrefix);
        return partitionIds;
    }

    private static final class RegisterPartitionedTableTask extends GridTaskAdapter {
        private static final long serialVersionUID = -7531450358494583759L;

        private final String[] tableNames;
        private final String templateTableNamePrefix;

        @GridRegistryResource
        private transient GridResourceRegistry registry;

        @SuppressWarnings("unchecked")
        protected RegisterPartitionedTableTask(GridJob job, String[] tableNames, String templateTableNamePrefix) {
            super(job, false);
            this.tableNames = tableNames;
            this.templateTableNamePrefix = templateTableNamePrefix;
        }

        @Override
        public boolean injectResources() {
            return true;
        }

        @Override
        protected int[] execute() throws GridException {
            int[] paritionIds = registerTables(tableNames, templateTableNamePrefix, registry);
            return paritionIds;
        }

    }
}