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
package gridool.directory.job;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.construct.GridJobBase;
import gridool.directory.helpers.DirectoryTaskAdapter;
import gridool.directory.ops.DropOperation;
import gridool.routing.GridTaskRouter;

import java.io.Serializable;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class DirectoryDropJob extends GridJobBase<DropOperation, Serializable> {
    private static final long serialVersionUID = -7886989119790120177L;

    public DirectoryDropJob() {}

    public Map<GridTask, GridNode> map(GridTaskRouter router, DropOperation ops)
            throws GridException {
        final GridNode[] nodes = router.getAllNodes();
        final int len = nodes.length;
        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(len);
        for(int i = 0; i < len; i++) {
            GridTask task = new DirectoryTaskAdapter(this, ops);
            map.put(task, nodes[i]);
        }
        return map;
    }

    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        return GridTaskResultPolicy.CONTINUE;
    }

    public Serializable reduce() throws GridException {
        return null;
    }
}
