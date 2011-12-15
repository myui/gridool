/*
 * @(#)$Id$
 *
 * Copyright 2010-2011 Makoto YUI
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
package gridool.sqlet;

import gridool.sqlet.SqletException.SqletErrorType;
import gridool.sqlet.api.CommandType;
import gridool.sqlet.api.Constants;
import gridool.sqlet.api.SqletCommand;
import gridool.sqlet.catalog.MapReduceConf;
import gridool.sqlet.catalog.MapReduceConf.Reducer;
import gridool.sqlet.catalog.PartitioningConf;
import gridool.sqlet.catalog.PartitioningConf.Partition;
import gridool.sqlet.mapred.MapShuffleSQLJob;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

/**
 * @author Makoto YUI
 */
public final class SqletCommandProcessor {

    private final SqletModule module;

    public SqletCommandProcessor(SqletModule module) {
        this.module = module;
    }

    public void executeAll() throws SqletException {
        SqletCommand cmd = module.pollCommand();
        while(cmd != null) {
            execute(cmd);
            cmd = module.pollCommand();
        }
    }

    public void execute(@Nonnull SqletCommand cmd) throws SqletException {
        CommandType type = cmd.getCmdType();
        switch(type) {
            case MAP_SHUFFLE:
                break;
            case MAP_NO_COMPILE:
                break;
            case REDUCE:
                break;
            default:
                throw new SqletException(SqletErrorType.execFailed, "Unsupported command type: "
                        + type);
        }
    }

    private void processMapShuffle(@Nonnull SqletCommand cmd) throws SqletException {
        String catalogName = cmd.getCatalogName();
        assert catalogName != null;

        MapReduceConf mrConf = module.getMapReduceConf(catalogName);
        PartitioningConf ptConf = module.getPartitioningConf(catalogName);

        if(mrConf == null) {
            throw new SqletException(SqletErrorType.execFailed, "MapReduceConf is not found: "
                    + catalogName);
        }
        if(ptConf == null) {
            throw new SqletException(SqletErrorType.execFailed, "PartitioningConf is not found: "
                    + catalogName);
        }

        String mapSelectQuery = cmd.getCommand();
        List<Partition> partitions = ptConf.getPartitions();
        List<Reducer> reducers = mrConf.getReducers();        
        Map<String, String> options = cmd.getProperties();
        String outputTblName = (options == null) ? null : options.get(Constants.PROP_OUTPUT_TBLNAME);
        
        MapShuffleSQLJob.JobConf jobConf = new MapShuffleSQLJob.JobConf(mapSelectQuery, partitions, reducers, outputTblName);
        
    }

}
