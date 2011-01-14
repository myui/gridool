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
import gridool.routing.GridRouter;
import gridool.util.GridUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
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
public class DBReduceJob extends GridJobBase<DBMapReduceJobConf, String> {
    private static final long serialVersionUID = -7029669717699601660L;
    private static final Log LOG = LogFactory.getLog(DBReduceJob.class);

    private transient String destTableName;

    public DBReduceJob() {}

    public Map<GridTask, GridNode> map(GridRouter router, DBMapReduceJobConf jobConf)
            throws GridException {
        final String inputTableName = jobConf.getMapOutputTableName();
        String destTableName = jobConf.getReduceOutputTableName();
        if(destTableName == null) {
            destTableName = generateOutputTableName(inputTableName, System.nanoTime());
            jobConf.setReduceOutputTableName(destTableName);
        }
        this.destTableName = destTableName;

        final GridNode[] nodes = router.getAllNodes();
        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(nodes.length);
        final String createTableTemplate = jobConf.getQueryTemplateForCreatingViewComposite();
        if(createTableTemplate != null) {
            final String dstDbUrl = jobConf.getReduceOutputDbUrl();
            if(dstDbUrl == null) {
                throw new GridException("ReduceOutputDestinationDbUrl should be specified when using a view in reduce phase");
            }
            final String outputTblName = jobConf.getReduceOutputTableName();
            final StringBuilder createTablesQuery = new StringBuilder(512);
            final StringBuilder createViewQuery = new StringBuilder(512);
            createViewQuery.append("CREATE VIEW ").append(outputTblName).append(" AS");
            final int numNodes = nodes.length;
            for(int i = 0; i < numNodes; i++) {
                if(i != 0) {
                    createViewQuery.append(" UNION ALL");
                }
                GridTask task = jobConf.makeReduceTask(this, inputTableName, destTableName);
                task.setTaskNumber(i+1);
                map.put(task, nodes[i]);
                String newTableName = GridUtils.generateTableName(outputTblName, task);
                String createTableQuery = createTableTemplate.replace("?", newTableName);
                createTablesQuery.append(createTableQuery).append("; ");
                createViewQuery.append(" SELECT * FROM ").append(newTableName);
            }
            createViewQuery.append(';');
            try {
                createView(dstDbUrl, createTablesQuery.toString(), createViewQuery.toString(), jobConf);
            } catch (SQLException e) {
                LOG.error(e.getMessage(), e);
                throw new GridException(e);
            }
        } else {
            for(GridNode node : nodes) {
                GridTask task = jobConf.makeReduceTask(this, inputTableName, destTableName);
                map.put(task, node);
            }
        }
        return map;
    }

    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        return GridTaskResultPolicy.CONTINUE;
    }

    public String reduce() throws GridException {
        return destTableName;
    }

    private static String generateOutputTableName(final String inputTableName, final long time) {
        final int endIndex = inputTableName.indexOf("__output");
        if(endIndex != -1) {
            String baseName = inputTableName.substring(0, endIndex);
            return baseName + "__output" + time;
        }
        return inputTableName + "__output" + time;
    }

    public static void createView(final String dstDbUrl, final String createTables, final String createView, final DBMapReduceJobConf jobConf)
            throws SQLException {
        final Connection conn;
        try {
            conn = jobConf.getConnection(dstDbUrl, true);
        } catch (ClassNotFoundException e) {
            throw new SQLException(e);
        }
        try {
            Statement st = conn.createStatement();
            st.executeUpdate(createTables);
            st.executeUpdate(createView);
            st.close();
            conn.commit();
            if(LOG.isInfoEnabled()) {
                LOG.info(createTables);
                LOG.info(createView);
            }
        } catch (SQLException sqle) {
            conn.rollback();
            throw sqle;
        } finally {
            conn.close();
        }
    }

}
