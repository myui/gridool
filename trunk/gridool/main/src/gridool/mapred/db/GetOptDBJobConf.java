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

import gridool.lib.db.DBRecord;
import gridool.lib.db.GenericDBRecord;
import gridool.mapred.db.task.DB2DhtMapShuffleTask;
import gridool.mapred.db.task.DBMapShuffleTaskBase;

import javax.annotation.Nonnull;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import xbird.util.lang.ObjectUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public abstract class GetOptDBJobConf extends DBMapReduceJobConf {
    private static final long serialVersionUID = 6329395278348057576L;

    @Option(name = "-driver", usage = "Class name of the database driver", required = true)
    private String driverClassName;

    @Option(name = "-connectUrl", usage = "database connect Url", required = true)
    private String dbConnectUrl;

    @Option(name = "-user", usage = "database user name")
    private String dbUserName = null;

    @Option(name = "-passwd", usage = "database password")
    private String dbPassword = null;

    @Option(name = "-mapTable", usage = "Table name for the outputs of mappers")
    private String mapOutputTableName = null;

    @Option(name = "-mapFields", usage = "Field names of the output table of mappers, seperated by comma")
    private String mapOutputFieldNames = null;

    @Option(name = "-dstDbUrl", usage = "database connect url in which recuce outputs are collected")
    private String reduceOutputDestinationDbUrl = null;

    @Option(name = "-reduceTable", usage = "Table name for the outputs of reducers")
    private String reduceOutputTableName = null;

    @Option(name = "-reduceFields", usage = "Field names of the output table of reducers, seperated by comma")
    private String reduceOutputFieldNames = null;

    @Option(name = "-viewTmpl", usage = "Query used for creating a view")
    private String createViewTemplate = null;

    @Option(name = "-inputQuery", usage = "The query used for the input of mappers", required = true)
    private String inputQuery;

    @Option(name = "-mapInputClass", usage = "Class name of map input records")
    private String mapInputRecordClass = null;

    public GetOptDBJobConf(@Nonnull String[] argv) {
        processArgs(argv, this);
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

    @Override
    public DBRecord createMapInputRecord() {
        if(mapInputRecordClass == null) {
            return new GenericDBRecord();
        } else {
            return ObjectUtils.instantiate(mapInputRecordClass);
        }
    }

    @Override
    public String getMapOutputTableName() {
        return mapOutputTableName;
    }

    @Override
    public String[] getMapOutputFieldNames() {
        return mapOutputFieldNames.split(",");
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
    public String getReduceOutputDestinationDbUrl() {
        return reduceOutputDestinationDbUrl;
    }

    @Override
    public String getQueryTemplateForCreatingViewComposite() {
        return createViewTemplate;
    }

    @SuppressWarnings("unchecked")
    @Override
    public DBMapShuffleTaskBase makeMapShuffleTask(DBMapJob dbMapJob, String destTableName) {
        return new DB2DhtMapShuffleTask(dbMapJob, this);
    }

    private static void processArgs(String[] args, Object target) {
        final CmdLineParser parser = new CmdLineParser(target);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

}
