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
package gridool.db;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.construct.GridJobBase;
import gridool.mapred.db.DBMapReduceJobConf;
import gridool.routing.GridRouter;

import java.util.IdentityHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class DBLocalJob extends GridJobBase<DBMapReduceJobConf, Boolean> {
    private static final long serialVersionUID = -2299324430472024910L;
    private static final Log LOG = LogFactory.getLog(DBLocalJob.class);
            
    private GridException error = null;

    public DBLocalJob() {}

    public Map<GridTask, GridNode> map(GridRouter router, DBMapReduceJobConf jobConf)
            throws GridException {
        Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(1);
        GridTask dbtask = jobConf.makeMapShuffleTask(this);
        GridNode localNode = getJobNode();
        map.put(dbtask, localNode);
        return map;
    }

    @Override
    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        final GridException ex = result.getException();
        if(ex != null) {
            this.error = ex;
            LOG.error("Exception caused while executing a local DB task", ex);
            return GridTaskResultPolicy.CANCEL_RETURN;
        }
        return GridTaskResultPolicy.CONTINUE;
    }

    public Boolean reduce() throws GridException {
        return error == null ? Boolean.TRUE : Boolean.FALSE;
    }

}
