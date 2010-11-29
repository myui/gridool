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

import gridool.GridClient;
import gridool.GridException;
import gridool.GridJob;
import gridool.GridTask;
import gridool.construct.GridJobBase;
import gridool.db.record.DBRecord;
import gridool.db.record.EmitDummyValueRecord;
import gridool.mapred.db.DBMapReduceJob;
import gridool.mapred.db.DBMapReduceJobConf;
import gridool.mapred.db.task.DB2DhtMapShuffleTask;
import gridool.mapred.db.task.DBMapShuffleTaskBase;
import gridool.mapred.db.task.Dht2DBGatherReduceTask;
import gridool.marshaller.GridMarshaller;

import java.io.OutputStream;
import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import xbird.util.datetime.StopWatch;
import xbird.util.net.NetUtils;
import xbird.util.string.StringUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class DBCountPageView {
    private static final Log LOG = LogFactory.getLog(DBCountPageView.class);

    public static void main(String[] args) {
        try {
            new DBCountPageView().run(args);
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void run(String[] args) throws Exception {
        DBCountJobConf jobConf = new DBCountJobConf();
        processArgs(args, jobConf);

        GridClient grid = new GridClient();
        long totalPageview = initialize(grid, jobConf);
        LOG.info("Initialized... totalPageview: " + totalPageview);

        Scanner kbd = new Scanner(System.in);
        String answer;
        do {
            System.out.println("Are you ready to run a Job? Type 'yes' to proceed.");
            answer = kbd.nextLine();
        } while(!"yes".equalsIgnoreCase(answer));

        LOG.info("Ready to run a MapReduce job! Go..");
        StopWatch sw = new StopWatch();
        runJob(grid, jobConf);
        sw.stop();

        boolean correct = verify(jobConf, totalPageview);
        if(correct) {
            LOG.info("Finished successfully in " + sw.toString() + "  :-)");
        } else {
            LOG.info("Finished abnormally in " + sw.toString() + "  ;-(");
            throw new RuntimeException("Evaluation was not correct!");
        }

    }

    private static void runJob(GridClient grid, DBCountJobConf jobConf) throws RemoteException {
        grid.execute(DBMapReduceJob.class, jobConf);
    }

    private static final class DBCountJobConf extends DBMapReduceJobConf {
        private static final long serialVersionUID = 1901162868874777896L;

        @Option(name = "-driver", usage = "Class name of the database driver")
        String driverClassName = "nl.cwi.monetdb.jdbc.MonetDriver";
        @Option(name = "-connectUrl", usage = "database connect Url")
        String dbConnectUrl = "jdbc:monetdb://localhost/URLAccess";
        @Option(name = "-user", usage = "database user name")
        String dbUserName = null;
        @Option(name = "-passwd", usage = "database password")
        String dbPassword = null;
        @Option(name = "-dstDbUrl", usage = "database connect url in which recuce outputs are collected")
        String reduceOutputDestinationDbUrl = "jdbc:monetdb://" + NetUtils.getLocalHostAddress()
                + "/URLAccess";
        @Option(name = "-inputQuery", usage = "The query used for the input of mappers")
        String inputQuery = "SELECT url, referrer, time FROM Access ORDER BY url";
        @Option(name = "-reduceTable", usage = "Table name for the outputs of reducers")
        String reduceOutputTableName = "Pageview";
        @Option(name = "-reduceFields", usage = "Field names of the output table of reducers, seperated by comma")
        String reduceOutputFieldNames = "url,pageview";
        @Option(name = "-viewTmpl", usage = "Query used for creating a view")
        String createViewTemplate = "CREATE TABLE ?(url VARCHAR(100) NOT NULL, pageview BIGINT NOT NULL, PRIMARY KEY (url))";

        public DBCountJobConf() {
            super();
        }

        @Override
        public String getDriverClassName() {
            return driverClassName;
        }

        @Override
        public String getConnectUrl() {
            return dbConnectUrl;
        }

        @Override
        public String getUserName() {
            return dbUserName;
        }

        @Override
        public String getPassword() {
            return dbPassword;
        }

        @Override
        public String getInputQuery() {
            return inputQuery;
        }

        @SuppressWarnings("unchecked")
        @Override
        public DBRecord createMapInputRecord() {
            return new EmitDummyValueRecord(1); //new GenericDBRecord(); //new AccessRecord();
        }

        @Override
        public String getReduceOutputTableName() {
            return reduceOutputTableName;
        }

        @Override
        public String[] getReduceOutputFieldNames() {
            return reduceOutputFieldNames.split(",");
        }

        @Override
        public String getReduceOutputDbUrl() {
            return reduceOutputDestinationDbUrl;
        }

        @Override
        public String getQueryTemplateForCreatingViewComposite() {
            return createViewTemplate;
        }

        @Override
        public DBMapShuffleTaskBase<DBRecord, DBRecord> makeMapShuffleTask(GridJobBase<DBMapReduceJobConf, ?> job) {
            return new PageviewMapper(job, this);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected GridTask makeReduceTask(GridJob job, String inputTableName, String destTableName) {
            return new PageviewReducer(job, inputTableName, destTableName, true, this);
        }

    }

    @SuppressWarnings("unused")
    private static final class AccessRecord implements DBRecord {
        private static final long serialVersionUID = -1192579060515200773L;

        String url;
        String referrer;
        long time;

        public byte[] getKey() {
            return StringUtils.getBytes(url);
        }

        public int getNumFields() {
            return 3;
        }

        public void readFields(ResultSet resultSet) throws SQLException {
            this.url = resultSet.getString(1);
            this.referrer = resultSet.getString(2);
            this.time = resultSet.getLong(3);
        }

        public void writeFields(PreparedStatement statement) throws SQLException {
            statement.setString(1, url);
            statement.setString(2, referrer);
            statement.setLong(3, time);
        }

        @SuppressWarnings("unchecked")
        public void writeTo(GridMarshaller marshaller, OutputStream out) throws GridException {
            marshaller.marshall(1, out);
        }
    }

    private static final class PageviewRecord implements DBRecord {
        private static final long serialVersionUID = 3327953182160549735L;

        String url;
        long pageview;

        public PageviewRecord(String url, long pageview) {
            this.url = url;
            this.pageview = pageview;
        }

        public byte[] getKey() {
            throw new IllegalStateException();
        }

        public int getNumFields() {
            return 2;
        }

        public void readFields(ResultSet resultSet) throws SQLException {
            this.url = resultSet.getString(1);
            this.pageview = resultSet.getLong(2);
        }

        public void writeFields(PreparedStatement statement) throws SQLException {
            statement.setString(1, url);
            statement.setLong(2, pageview);
        }

        @SuppressWarnings("unchecked")
        public void writeTo(GridMarshaller marshaller, OutputStream out) throws GridException {
            throw new IllegalStateException();
        }
    }

    @Deprecated
    private static final class PageviewMapper extends DB2DhtMapShuffleTask {
        private static final long serialVersionUID = 6919810831471575121L;

        @SuppressWarnings("unchecked")
        public PageviewMapper(GridJob job, DBMapReduceJobConf jobConf) {
            super(job, jobConf);
        }

        @Override
        protected boolean process(DBRecord record) {
            shuffle(record);
            return true;
        }
    }

    private static final class PageviewReducer extends Dht2DBGatherReduceTask<Integer> {
        private static final long serialVersionUID = 1085020458148110182L;

        @SuppressWarnings("unchecked")
        public PageviewReducer(GridJob job, String inputDhtName, String destDhtName, boolean removeInputDhtOnFinish, DBMapReduceJobConf jobConf) {
            super(job, inputDhtName, destDhtName, removeInputDhtOnFinish, jobConf);
        }

        @Override
        protected boolean processRecord(byte[] key, Iterator<Integer> values) {
            long sum = 0L;
            while(values.hasNext()) {
                sum += values.next();
            }
            DBRecord r = new PageviewRecord(StringUtils.toString(key), sum);
            collectOutput(r);
            return true;
        }

    }

    private static void processArgs(String[] args, Object target) {
        final CmdLineParser parser = new CmdLineParser(target);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            System.exit(1);
        }
    }

    private static long initialize(GridClient grid, DBMapReduceJobConf jobConf) throws Exception {
        Long pageview = grid.execute(DbCountInitializeJob.class, jobConf);
        return pageview.intValue();
    }

    private static boolean verify(final DBMapReduceJobConf jobConf, final long totalPageview)
            throws SQLException {
        //check total num pageview
        String dbUrl = jobConf.getReduceOutputDbUrl();
        final Connection conn;
        try {
            conn = jobConf.getConnection(dbUrl, true);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
        String sumPageviewQuery = "SELECT SUM(pageview) FROM Pageview";
        Statement st = null;
        ResultSet rs = null;
        try {
            st = conn.createStatement();
            rs = st.executeQuery(sumPageviewQuery);
            rs.next();
            long sumPageview = rs.getLong(1);

            LOG.info("totalPageview=" + totalPageview);
            LOG.info("sumPageview=" + sumPageview);

            return totalPageview == sumPageview && totalPageview != 0;
        } finally {
            if(st != null) {
                st.close();
            }
            if(rs != null) {
                rs.close();
            }
            conn.close();
        }
    }

}
