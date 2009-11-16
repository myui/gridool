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
package gridool.mapred.db;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.construct.GridJobBase;
import gridool.mapred.db.task.DBMapShuffleTask;
import gridool.routing.GridTaskRouter;

import java.util.IdentityHashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import xbird.util.lang.ObjectUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public class DBMapJob extends GridJobBase<byte[], String> {
    private static final long serialVersionUID = 1965382841993527705L;

    protected transient DBMapReduceJobConf jobConf;
    protected transient String mapOutputTableName;
    
    public DBMapJob() {
        super();
    }

    public Map<GridTask, GridNode> map(GridTaskRouter router, byte[] rawLogic) throws GridException {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        DBMapReduceJobConf logic = ObjectUtils.readObjectQuietly(rawLogic, cl);
        this.jobConf = logic;
        String destTableName = generateMapOutputTableName();
        logic.setMapOutputTableName(destTableName);
        this.mapOutputTableName = destTableName;
        
        final GridNode[] nodes = router.getAllNodes();
        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(nodes.length);
        for(GridNode node : nodes) {
            DBMapShuffleTask task = logic.makeMapShuffleTask(this, destTableName);
            map.put(task, node);
        }
        return map;
    }

    public GridTaskResultPolicy result(GridTask task, GridTaskResult result) throws GridException {
        return GridTaskResultPolicy.CONTINUE;
    }

    @Nonnull
    public String reduce() throws GridException {
        return mapOutputTableName;
    }
    
    private static String generateMapOutputTableName() {
        return "mr_mapoutput_" + System.nanoTime();
    }

}
