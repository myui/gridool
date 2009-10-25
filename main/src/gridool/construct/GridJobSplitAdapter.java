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
package gridool.construct;

import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Map;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.routing.GridTaskRouter;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public abstract class GridJobSplitAdapter<A, R> extends GridJobBase<A, R> {
    private static final long serialVersionUID = -1402492823168802455L;

    public GridJobSplitAdapter() {}

    protected abstract Collection<? extends GridTask> split(int gridSize, A arg)
            throws GridException;

    public Map<GridTask, GridNode> map(GridTaskRouter router, A arg) throws GridException {
        assert (router != null);

        int gridSize = router.getGridSize();
        Collection<? extends GridTask> tasks = split(gridSize, arg);

        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(tasks.size());
        for(GridTask task : tasks) {
            GridNode mappedNode = router.selectNode(task);
            map.put(task, mappedNode);
        }
        return map;
    }

}
