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
package gridool.db.sql;

import gridool.GridException;
import gridool.GridJob;
import gridool.GridNode;
import gridool.construct.GridTaskAdapter;
import gridool.db.catalog.DistributionCatalog;
import gridool.routing.GridTaskRouter;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class ParallelSQLMapTask extends GridTaskAdapter {
    private static final long serialVersionUID = 2478800827882047565L;

    @Nonnull
    private final GridNode masterNode;

    private transient DistributionCatalog catalog;

    @SuppressWarnings("unchecked")
    public ParallelSQLMapTask(GridJob job, GridNode masterNode, DistributionCatalog catalog) {
        super(job, true);
        this.masterNode = masterNode;
        this.catalog = catalog;
    }

    @Override
    public List<GridNode> listFailoverCandidates(GridNode localNode, GridTaskRouter router) {
        GridNode[] slaves = catalog.getSlaves(masterNode, DistributionCatalog.defaultDistributionKey);
        return Arrays.asList(slaves);
    }

    @Override
    protected GridNode execute() throws GridException {
        // TODO
        return masterNode;
    }

}
