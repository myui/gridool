/*
 * @(#)$Id$
 *
 * Copyright 2009-2010 Makoto YUI
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
package gridool.sqlet.partitioning;

import gridool.GridNode;
import gridool.util.GridUtils;
import gridool.util.jdbc.JDBCUtils;
import gridool.util.jdbc.ResultSetHandler;

import java.io.File;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class PartitioningConf implements Serializable {
    private static final long serialVersionUID = 7009375306837374901L;
    private static final Log LOG = LogFactory.getLog(PartitioningConf.class);

    private final List<Partition> list;

    public PartitioningConf() {
        this.list = new ArrayList<Partition>(8);
    }
    
    @Nonnull
    public List<Partition> getPartitions() {
        return list;
    }

    public void loadSettings(File file) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    public void loadSettings(Connection conn, String defTable, String srcTable) throws SQLException {
        String sql1 = "SELECT tblName, host, port, dburl, mapOutput FROM " + defTable
                + " WHERE srcTable = " + srcTable + " and master IS TRUE";
        final Map<String, Partition> map = new HashMap<String, Partition>(128);
        ResultSetHandler rsh1 = new ResultSetHandler() {
            public Object handle(ResultSet rs) throws SQLException {
                while(rs.next()) {
                    String tblName = rs.getString(1);
                    String host = rs.getString(2);
                    int port = rs.getInt(3);
                    GridNode hostNode = GridUtils.getNode(host, port);
                    String dbUrl = rs.getString(4);
                    String mapOutput = rs.getString(5);
                    Partition p = new Partition(tblName, true, hostNode, dbUrl, mapOutput);
                    list.add(p);
                    map.put(tblName, p);
                }
                return null;
            }
        };
        JDBCUtils.query(conn, sql1, rsh1);
        String sql2 = "SELECT tblName, host, port, dburl, mapOutput FROM " + defTable
                + " WHERE srcTable = " + srcTable + " and master IS FALSE";
        ResultSetHandler rsh2 = new ResultSetHandler() {
            public Object handle(ResultSet rs) throws SQLException {
                while(rs.next()) {
                    String tblName = rs.getString(1);
                    String host = rs.getString(2);
                    int port = rs.getInt(3);
                    GridNode hostNode = GridUtils.getNode(host, port);
                    String dbUrl = rs.getString(4);
                    String mapOutput = rs.getString(5);
                    Partition slave = new Partition(tblName, false, hostNode, dbUrl, mapOutput);
                    Partition master = map.get(tblName);
                    if(master == null) {
                        LOG.error("Master partition is not found for slave: " + slave);
                    } else {
                        master.addSlave(slave);
                    }
                }
                return null;
            }
        };
        JDBCUtils.query(conn, sql2, rsh2);
    }

    public static final class Partition implements Serializable {
        private static final long serialVersionUID = -8774183764667248705L;

        final String tblName;
        final boolean master;
        final GridNode host;
        final String dbUrl;
        final String mapOutput;

        @Nullable
        final List<Partition> slaves;

        Partition(String tblName, boolean master, GridNode host, String dbUrl, String mapOutput) {
            super();
            this.tblName = tblName;
            this.master = master;
            this.host = host;
            this.dbUrl = dbUrl;
            this.mapOutput = mapOutput;
            this.slaves = master ? new LinkedList<Partition>() : null;
        }

        public void addSlave(Partition slave) {
            if(slave.master) {
                throw new IllegalArgumentException("Illegal as slave: " + slave);
            }
            if(slave.tblName != tblName) {
                throw new IllegalArgumentException("Partitioned table name differs");
            }
            slave.addSlave(slave);
        }

        @Override
        public String toString() {
            return "Partition [tblName=" + tblName + ", master=" + master + ", host=" + host
                    + ", dbUrl=" + dbUrl + ", mapOutput=" + mapOutput + ", slaves=" + slaves + "]";
        }

    }

}
