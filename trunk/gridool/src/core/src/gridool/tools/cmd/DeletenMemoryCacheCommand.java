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
package gridool.tools.cmd;

import gridool.Grid;
import gridool.GridClient;
import gridool.GridException;
import gridool.GridJob;
import gridool.GridNode;
import gridool.GridResourceRegistry;
import gridool.GridTask;
import gridool.annotation.GridRegistryResource;
import gridool.construct.GridJobBase;
import gridool.construct.GridTaskAdapter;
import gridool.db.partitioning.csv.distmm.InMemoryMappingIndex;
import gridool.routing.GridRouter;
import gridool.util.cmdline.CommandBase;
import gridool.util.cmdline.CommandException;
import gridool.util.lang.ArrayUtils;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * delete cache NAME1 NAME2 .. NAMEn
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class DeletenMemoryCacheCommand extends CommandBase {

    public DeletenMemoryCacheCommand() {}

    @Override
    public boolean match(String[] args) {
        if(args.length < 3) {
            return false;
        }
        if(!"delete".equalsIgnoreCase(args[0])) {
            return false;
        }
        if(!"cache".equalsIgnoreCase(args[1])) {
            return false;
        }
        return true;
    }

    @Override
    public boolean process(String[] args) throws CommandException {
        assert (args.length >= 3);
        final String[] cacheNames = ArrayUtils.copyOfRange(args, 2, args.length);
        final Grid grid = new GridClient();
        try {
            grid.execute(RemoveInMemoryCacheJob.class, cacheNames);
        } catch (RemoteException e) {
            throw new CommandException(e);
        }
        return false;
    }

    public static final class RemoveInMemoryCacheJob extends GridJobBase<String[], Boolean> {
        private static final long serialVersionUID = 3044729308092240579L;

        public RemoveInMemoryCacheJob() {
            super();
        }

        @Override
        public Map<GridTask, GridNode> map(GridRouter router, String[] cacheNames)
                throws GridException {
            final GridNode[] nodes = router.getAllNodes();
            final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(nodes.length);
            for(GridNode node : nodes) {
                GridTask task = new RemoveInMemoryCacheTask(this, cacheNames);
                map.put(task, node);
            }
            return map;
        }

        @Override
        public Boolean reduce() throws GridException {
            return Boolean.TRUE;
        }

    }

    private static final class RemoveInMemoryCacheTask extends GridTaskAdapter {
        private static final long serialVersionUID = 1821766932451103810L;

        private final String[] cacheNames;

        @GridRegistryResource
        private transient GridResourceRegistry registry;

        @SuppressWarnings("unchecked")
        protected RemoveInMemoryCacheTask(GridJob job, String[] cacheNames) {
            super(job, false);
            this.cacheNames = cacheNames;
        }

        @Override
        public boolean injectResources() {
            return true;
        }

        @Override
        protected Serializable execute() throws GridException {
            InMemoryMappingIndex index = registry.getMappingIndex();
            for(String idxName : cacheNames) {
                index.clearIndex(idxName);
            }
            return null;
        }

    }

    @Override
    public String usage() {
        return constructHelp("delete in-memory cache", "delete cache NAME1 NAME2 .. NAMEn");
    }

}
