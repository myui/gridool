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
package gridool.sqlet.env;

import gridool.GridNode;
import gridool.sqlet.SqletException;
import gridool.sqlet.SqletException.ErrorType;
import gridool.util.GridUtils;
import gridool.util.csv.HeaderAwareCsvReader;
import gridool.util.io.FastBufferedInputStream;
import gridool.util.io.IOUtils;
import gridool.util.jdbc.JDBCUtils;
import gridool.util.jdbc.ResultSetHandler;
import gridool.util.lang.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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

    public void loadSettings(@Nonnull String uri) throws SqletException {
        if(uri.endsWith(".csv")) {
            final InputStream is;
            try {
                is = IOUtils.openStream(uri);
            } catch (IOException e) {
                throw new SqletException(ErrorType.configFailed, "Illegal URI format: " + uri, e);
            }
            InputStreamReader reader = new InputStreamReader(new FastBufferedInputStream(is));
            HeaderAwareCsvReader csvReader = new HeaderAwareCsvReader(reader, ',', '"');

            final Map<String, Integer> headerMap;
            try {
                headerMap = csvReader.parseHeader();
            } catch (IOException e) {
                throw new SqletException(ErrorType.configFailed, "failed to parse a header: " + uri, e);
            }

            final int[] fieldIndexes = toFieldIndexes(headerMap);
            final Map<String, Partition> masterSlave = new HashMap<String, Partition>(128);
            while(csvReader.next()) {
                String tblName = csvReader.get(fieldIndexes[0]);
                String srcTable = csvReader.get(fieldIndexes[1]);
                String masterStr = csvReader.get(fieldIndexes[2]);
                String host = csvReader.get(fieldIndexes[3]);
                String portStr = csvReader.get(fieldIndexes[4]);
                String dbUrl = csvReader.get(fieldIndexes[5]);
                String mapOutput = csvReader.get(fieldIndexes[6]);

                Preconditions.checkNotNull(tblName, srcTable, masterStr, host, portStr, dbUrl);

                int port = Integer.parseInt(portStr);
                GridNode hostNode = GridUtils.getNode(host, port);
                final boolean master = Boolean.parseBoolean(masterStr);
                final Partition p = new Partition(tblName, master, hostNode, dbUrl, mapOutput);
                if(master) {
                    masterSlave.put(tblName, p);
                    list.add(p);
                } else {
                    Partition masterPartition = masterSlave.get(tblName);
                    if(masterPartition == null) {
                        LOG.error("Master partition is not found for slave: " + p);
                    } else {
                        masterPartition.addSlave(p);
                    }
                }
            }
        } else {
            throw new IllegalArgumentException("Unsupported URI: " + uri);
        }
    }

    private static int[] toFieldIndexes(@Nullable Map<String, Integer> map) {
        if(map == null) {
            return new int[] { 0, 1, 2, 3, 4, 5, 6 };
        } else {
            Integer c0 = map.get("TBLNAME");
            Integer c1 = map.get("SRCTABLE");
            Integer c2 = map.get("MASTER");
            Integer c3 = map.get("HOST");
            Integer c4 = map.get("PORT");
            Integer c5 = map.get("DBURL");
            Integer c6 = map.get("MAPOUTPUT");

            Preconditions.checkNotNull(c0, c1, c2, c3, c4, c5, c6);

            final int[] indexes = new int[7];
            indexes[0] = c0.intValue();
            indexes[1] = c1.intValue();
            indexes[2] = c2.intValue();
            indexes[3] = c3.intValue();
            indexes[4] = c4.intValue();
            indexes[5] = c5.intValue();
            indexes[6] = c6.intValue();
            return indexes;
        }
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

        @Nonnull
        final String tblName;
        final boolean master;
        @Nonnull
        final GridNode host;
        @Nonnull
        final String dbUrl;
        @Nullable
        final String mapOutput;

        @Nullable
        final List<Partition> slaves;

        Partition(@Nonnull String tblName, boolean master, @Nonnull GridNode host, @Nonnull String dbUrl, @Nullable String mapOutput) {
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
            if(!slave.tblName.equals(tblName)) {
                throw new IllegalArgumentException("Partitioned table name differs");
            }
            slaves.add(slave);
        }

        @Override
        public String toString() {
            return "Partition [tblName=" + tblName + ", master=" + master + ", host=" + host
                    + ", dbUrl=" + dbUrl + ", mapOutput=" + mapOutput + ", slaves=" + slaves + "]";
        }

    }

}
