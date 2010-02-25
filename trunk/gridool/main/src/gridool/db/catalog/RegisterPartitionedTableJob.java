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
import gridool.annotation.GridConfigResource;
import gridool.annotation.GridRegistryResource;
import gridool.construct.GridJobBase;
import gridool.construct.GridTaskAdapter;
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
public final class RegisterPartitionedTableJob extends GridJobBase<String[], int[]> {
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

    public Map<GridTask, GridNode> map(GridTaskRouter router, String[] tableNames)
            throws GridException {
        final int[] partitionIds = registerTables(tableNames, registry);

        final GridNode localNode = config.getLocalNode();
        final GridNode[] nodes = router.getAllNodes();
        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(nodes.length);
        for(final GridNode node : nodes) {
            if(!node.equals(localNode)) {
                GridTask task = new RegisterPartitionedTableTask(this, tableNames);
                map.put(task, node);
            }
        }

        this.partitionIds = partitionIds;
        return map;
    }

    public int[] reduce() throws GridException {
        return partitionIds;
    }

    private static int[] registerTables(final String[] tableNames, final GridResourceRegistry registry)
            throws GridException {
        final int numTables = tableNames.length;
        final int[] partitionIds = new int[numTables];
        final DistributionCatalog catalog = registry.getDistributionCatalog();
        for(int i = 0; i < numTables; i++) {
            String tblname = tableNames[i];
            partitionIds[i] = catalog.bindTableId(tblname);
        }
        return partitionIds;
    }

    private static final class RegisterPartitionedTableTask extends GridTaskAdapter {
        private static final long serialVersionUID = -7531450358494583759L;

        private final String[] tableNames;

        @GridRegistryResource
        private transient GridResourceRegistry registry;

        @SuppressWarnings("unchecked")
        protected RegisterPartitionedTableTask(GridJob job, String[] tableNames) {
            super(job, false);
            this.tableNames = tableNames;
        }

        @Override
        public boolean injectResources() {
            return true;
        }

        @Override
        protected int[] execute() throws GridException {
            int[] paritionIds = registerTables(tableNames, registry);
            return paritionIds;
        }

    }
}