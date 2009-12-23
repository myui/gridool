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
package gridool.db.partitioning;

import javax.annotation.Nonnull;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public class GetOptDBPartitioningJobConf extends DBPartitioningJobConf {
    private static final long serialVersionUID = -2839327000541349951L;

    @Option(name = "-driver", usage = "Class name of the database driver (Required)", required = true)
    private String driverClassName;

    @Option(name = "-connectUrl", usage = "database connect Url (Required)", required = true)
    private String dbConnectUrl;

    @Option(name = "-table", usage = "Table name into which copy records", required = true)
    private String tableName;

    @Option(name = "-csv", usage = "File path to csv file", required = true)
    private String csvFilePath;

    @Option(name = "-createTbl", usage = "DDL for creating a table to load data", required = true)
    private String createTableDDL;

    // ----------------------------
    // optional stuffs

    @Option(name = "-user", usage = "database user name")
    private String dbUserName = null;

    @Option(name = "-passwd", usage = "database password")
    private String dbPassword = null;

    @Option(name = "-fieldSep", usage = "Field separator used in the CSV file")
    private char fieldSeparator = '\t';

    @Option(name = "-recSep", usage = "Record separator used in the CSV file")
    private char recordSeparator = '\n';

    @Option(name = "-quote", usage = "Quote string used in the CSV file")
    private char stringQuote = '\"';

    // ----------------------------

    public GetOptDBPartitioningJobConf(@Nonnull String[] argv) {
        processArgs(argv, this);
    }

    @Override
    public final String getDriverClassName() {
        return driverClassName;
    }

    @Override
    public final String getConnectUrl() {
        return dbConnectUrl;
    }

    @Override
    public final String getTableName() {
        return tableName;
    }

    @Override
    public final String getCsvFilePath() {
        return csvFilePath;
    }

    @Override
    public final String getCreateTableDDL() {
        return createTableDDL;
    }

    // -----------------------------------------
    // optional stuffs

    @Override
    public final String getUserName() {
        return dbUserName;
    }

    @Override
    public final String getPassword() {
        return dbPassword;
    }

    @Override
    public final char getFieldSeparator() {
        return fieldSeparator;
    }

    @Override
    public final char getRecordSeparator() {
        return recordSeparator;
    }

    @Override
    public final char getStringQuote() {
        return stringQuote;
    }

    private static void processArgs(String[] args, Object target) {
        final CmdLineParser parser = new CmdLineParser(target);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            parser.printUsage(System.err);
            throw new IllegalArgumentException(e);
        }
    }
}
