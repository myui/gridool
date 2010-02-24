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
import gridool.db.sql.ParallelSQLExecJob.OutputMethod;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.RemoteException;

import xbird.util.cmdline.CommandBase;
import xbird.util.cmdline.CommandException;
import xbird.util.cmdline.Option.BooleanOption;
import xbird.util.cmdline.Option.IntOption;
import xbird.util.cmdline.Option.StringOption;
import xbird.util.datetime.StopWatch;
import xbird.util.io.FileUtils;
import xbird.util.io.IOUtils;

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
        addOption(new StringOption("outputMethod", "csvfile", false));
        addOption(new IntOption("waitSPE", -1, false));
        addOption(new BooleanOption("showOutput", false, false));
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
        String method = getOption("outputMethod");
        OutputMethod outputMethod = OutputMethod.resolve(method);
        Integer waitSPE = getOption("waitSPE");
        long waitInMills = (waitSPE.intValue() == -1) ? -1L : (waitSPE.longValue() * 1000L);
        ParallelSQLExecJob.JobConf jobConf = new ParallelSQLExecJob.JobConf(outputTable, mapQuery, reduceQuery, outputMethod, waitInMills);

        final StopWatch sw = new StopWatch();
        final Grid grid = new GridClient();
        final String outputName;
        try {
            outputName = grid.execute(ParallelSQLExecJob.class, jobConf);
        } catch (RemoteException e) {
            throw new CommandException(e);
        }
        System.out.println("ParallelSQLExecJob [outputName=" + outputName + ", outputType="
                + outputMethod + "] finished in " + sw + '\n');

        Boolean showOutput = getOption("showOutput");
        if(showOutput.booleanValue()) {
            if(outputMethod == OutputMethod.csvFile) {
                try {
                    IOUtils.copy(new FileInputStream(outputName), System.out);
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println();
            }
        }        
        return true;
    }

    public String usage() {
        return constructHelp("Executes a parallel SQL job", "-mapQuery FILENAME -reduceQuery FILENAME -outputTable NAME [-asView true] [-waitSPE SECONDS] execute sql");
    }

}
