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
package gridool.tools.cmd;

import gridool.Grid;
import gridool.GridClient;
import gridool.db.sql.ParallelSQLExecJob;

import java.io.File;
import java.rmi.RemoteException;

import xbird.util.cmdline.CommandBase;
import xbird.util.cmdline.CommandException;
import xbird.util.cmdline.Option.BooleanOption;
import xbird.util.cmdline.Option.StringOption;
import xbird.util.datetime.StopWatch;
import xbird.util.io.FileUtils;

/**
 * -mapquery FILENAME -reducequery FILENAME -outputTable NAME [-asview true] [-waitSPE SECONDS] execute sql
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class ParallelSQLExecCommand extends CommandBase {

    public ParallelSQLExecCommand() {
        addOption(new StringOption("mapQuery", true));
        addOption(new StringOption("reduceQuery", true));
        addOption(new StringOption("outputTable", null, false));
        addOption(new BooleanOption("asView", Boolean.FALSE, false));
    }

    public boolean match(String[] args) {
        if(args.length != 2) {
            return false;
        }
        if(!"execute".equals(args[0])) {
            return false;
        }
        if(!"sql".equals(args[1])) {
            return false;
        }
        return true;
    }

    public boolean process(String[] args) throws CommandException {
        String mapFp = getOption("mapQuery");
        String reduceFp = getOption("reduceQuery");
        assert (mapFp != null);
        assert (reduceFp != null);
        File mapFile = new File(mapFp);
        File reduceFile = new File(reduceFp);
        if(!mapFile.exists()) {
            throw new CommandException("Map query file does not exist: "
                    + mapFile.getAbsolutePath());
        }
        if(!reduceFile.exists()) {
            throw new CommandException("Reduce query file does not exist: "
                    + reduceFile.getAbsolutePath());
        }
        String mapQuery = FileUtils.toString(mapFile);
        String reduceQuery = FileUtils.toString(reduceFile);
        String outputTable = getOption("outputTable");
        Boolean asView = getOption("asView");
        Integer waitSPE = getOption("waitSPE");
        long waitInMills = waitSPE.longValue() * 1000L;
        ParallelSQLExecJob.JobConf jobConf = new ParallelSQLExecJob.JobConf(outputTable, mapQuery, reduceQuery, asView, waitInMills);

        final StopWatch sw = new StopWatch();
        final Grid grid = new GridClient();
        final String actualOuputTable;
        try {
            actualOuputTable = grid.execute(ParallelSQLExecJob.class, jobConf);
        } catch (RemoteException e) {
            throw new CommandException(e);
        }
        System.out.println("ParallelSQLExecJob [outputTable=" + actualOuputTable + "] finished in "
                + sw);
        return true;
    }

    public String usage() {
        return constructHelp("Executes a parallel SQL job", "-mapquery FILENAME -reducequery FILENAME -outputTable NAME [-asview true] [-waitSPE SECONDS] execute sql");
    }

}
