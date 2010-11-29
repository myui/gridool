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
package dbcount;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.construct.GridJobBase;
import gridool.construct.GridTaskAdapter;
import gridool.mapred.db.DBMapReduceJobConf;
import gridool.routing.GridRouter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * <DIV lang="en"></DIV> <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class DbCountInitializeJob extends GridJobBase<DBMapReduceJobConf, Long> {
    private static final long serialVersionUID = -2291873883401731077L;
    private static final Log LOG = LogFactory.getLog(DbCountInitializeJob.class);

    private transient final AtomicLong totalPageviews = new AtomicLong(0);

    public DbCountInitializeJob() {}

    @SuppressWarnings("serial")
    public Map<GridTask, GridNode> map(final GridRouter router, final DBMapReduceJobConf jobConf)
            throws GridException {
        final GridNode[] nodes = router.getAllNodes();
        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(nodes.length);
        for(GridNode node : nodes) {
            GridTask task = new GridTaskAdapter(this, false) {
                protected Integer execute() throws GridException {
                    final int pageview;
                    try {
                        pageview = initialize(jobConf);
                    } catch (Exception e) {
                        LOG.error(e.getMessage(), e);
                        throw new GridException(e);
                    }
                    return pageview;
                }
            };
            map.put(task, node);
        }
        return map;
    }

    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        Integer pageviews = result.getResult();
        if(pageviews != null) {
            totalPageviews.addAndGet(pageviews.longValue());
        }
        return GridTaskResultPolicy.CONTINUE;
    }

    public Long reduce() throws GridException {
        return totalPageviews.get();
    }

    private int initialize(DBMapReduceJobConf jobConf) throws ClassNotFoundException, SQLException {
        Connection conn = jobConf.getConnection(true);
        boolean useView = jobConf.getQueryTemplateForCreatingViewComposite() != null;
        dropTables(conn, useView);
        createTables(conn, useView);
        return populateAccess(conn);
    }

    private void dropTables(final Connection conn, final boolean useView) {
        final String dropAccess = "DROP TABLE Access";
        final String dropPageview = "DROP TABLE Pageview";
        try {
            Statement st = conn.createStatement();
            st.executeUpdate(dropAccess);
            if(!useView) {
                st.executeUpdate(dropPageview);
            }
            conn.commit();
            st.close();
        } catch (SQLException ex) {// ignore
            try {
                conn.rollback();
            } catch (SQLException e) {
                ;
            }
        }
    }

    private void createTables(final Connection conn, final boolean useView) throws SQLException {
        final String createAccess = "CREATE TABLE " + "Access(url VARCHAR(100) NOT NULL,"
                + " referrer VARCHAR(100)," + " time BIGINT NOT NULL,"
                + " PRIMARY KEY (url, time))";
        final String createPageview = "CREATE TABLE " + "Pageview(url VARCHAR(100) NOT NULL,"
                + " pageview BIGINT NOT NULL," + " PRIMARY KEY (url))";

        Statement st = conn.createStatement();
        try {
            st.executeUpdate(createAccess);
            if(!useView) {
                st.executeUpdate(createPageview);
            }
            conn.commit();
        } finally {
            st.close();
        }
    }

    private int populateAccess(final Connection conn) throws SQLException {
        final Random random = new Random();

        final int PROBABILITY_PRECISION = 100; // 1 / 100
        final int NEW_PAGE_PROBABILITY = 15; // 15 / 100

        // Pages in the site :
        final String[] pages = { "/a", "/b", "/c", "/d", "/e", "/f", "/g", "/h", "/i", "/j" };
        // linkMatrix[i] is the array of pages(indexes) that page_i links to.
        final int[][] linkMatrix = { { 1, 5, 7 }, { 0, 7, 4, 6, }, { 0, 1, 7, 8 },
                { 0, 2, 4, 6, 7, 9 }, { 0, 1 }, { 0, 3, 5, 9 }, { 0 }, { 0, 1, 3 }, { 0, 2, 6 },
                { 0, 2, 6 } };

        int totalPageview = 0;
        PreparedStatement statement = null;
        try {
            statement = conn.prepareStatement("INSERT INTO Access(url, referrer, time) VALUES (?, ?, ?)");

            int currentPage = random.nextInt(pages.length);
            String referrer = null;

            final int time = random.nextInt(50) + 50;
            for(int i = 0; i < time; i++) {
                statement.setString(1, pages[currentPage]);
                if(referrer == null) {
                    statement.setNull(2, Types.VARCHAR);
                } else {
                    statement.setString(2, referrer);
                }
                statement.setLong(3, i);
                statement.execute();
                ++totalPageview;

                // go to a new page with probability NEW_PAGE_PROBABILITY /
                // PROBABILITY_PRECISION
                int action = random.nextInt(PROBABILITY_PRECISION);
                if(action < NEW_PAGE_PROBABILITY) {
                    currentPage = random.nextInt(pages.length); // a random page
                    referrer = null;
                } else {
                    referrer = pages[currentPage];
                    action = random.nextInt(linkMatrix[currentPage].length);
                    currentPage = linkMatrix[currentPage][action];
                }
            }

            conn.commit();

        } catch (SQLException ex) {
            conn.rollback();
            throw ex;
        } finally {
            if(statement != null) {
                statement.close();
            }
        }
        return totalPageview;
    }

}
