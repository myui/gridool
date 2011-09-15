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
import gridool.directory.ops.CreateOperation;
import gridool.routing.GridRouter;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class DirectoryCreateJob extends GridJobBase<CreateOperation, Boolean> {
    private static final long serialVersionUID = -2999620281598151649L;

    private transient boolean suceedAll = true;

    public DirectoryCreateJob() {}

    public Map<GridTask, GridNode> map(GridRouter router, CreateOperation ops)
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
        Boolean suceed = result.getResult();
        if(suceed == null || suceed.booleanValue() == false) {
            this.suceedAll = false;
            return GridTaskResultPolicy.RETURN;
        }
        return GridTaskResultPolicy.CONTINUE;
    }

    public Boolean reduce() throws GridException {
        return suceedAll;
    }

}
