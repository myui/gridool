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
import gridool.GridNode;
import gridool.db.sql.ParallelSQLExecJob;
import gridool.db.sql.ParallelSQLExecJob.OutputMethod;
import gridool.util.cmdline.CommandBase;
import gridool.util.cmdline.CommandException;
import gridool.util.cmdline.Option.BooleanOption;
import gridool.util.cmdline.Option.IntOption;
import gridool.util.cmdline.Option.StringOption;
import gridool.util.datetime.StopWatch;
import gridool.util.io.FileUtils;
import gridool.util.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.RemoteException;


/**
 * -mapquery FILENAME -reducequery FILENAME -outputName NAME [-preAggrMap FILENAME -preAggrReduce FILENAME] [-outputMethod NAME] [-waitSPE SECONDS] execute sql
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class ParallelSQLExecCommand extends CommandBase {

    public ParallelSQLExecCommand() {
        addOption(new StringOption("mapQuery", true));
        addOption(new StringOption("reduceQuery", true));
        addOption(new StringOption("outputName", null, false));
        addOption(new StringOption("outputMethod", "csvfile", false));
        addOption(new StringOption("preAggrMap", false));
        addOption(new StringOption("preAggrReduce", false));
        addOption(new IntOption("waitSPE", -1, false));
        addOption(new BooleanOption("showOutput", false, false));
        addOption(new BooleanOption("failover", true, false));
        addOption(new BooleanOption("delegate", false, false));
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
        String outputName = getOption("outputName");
        String method = getOption("outputMethod");
        OutputMethod outputMethod = OutputMethod.resolve(method);
        Integer waitSPE = getOption("waitSPE");
        long waitInMills = (waitSPE.intValue() == -1) ? -1L : (waitSPE.longValue() * 1000L);
        Boolean failoverActive = getOption("failover");
        Boolean delegate = getOption("delegate");

        Grid grid = new GridClient();
        if(delegate.booleanValue()) {
            final GridNode delegatedNode;
            try {
                delegatedNode = grid.delegate(false);
            } catch (RemoteException e) {
                throw new CommandException("failed to delegate", e);
            }
            grid = new GridClient(delegatedNode);
        }
        final StopWatch sw = new StopWatch();
        sw.setShowInSec(true);

        String preAggrMapFp = getOption("preAggrMap");
        String preAggrReduceFp = getOption("preAggrReduce");
        if(preAggrMapFp != null && preAggrReduceFp != null) {
            mapQuery = runPreAggregation(grid, mapQuery, preAggrMapFp, preAggrReduceFp, waitInMills, failoverActive.booleanValue());
        }

        final ParallelSQLExecJob.JobConf jobConf = new ParallelSQLExecJob.JobConf(outputName, mapQuery, reduceQuery, outputMethod, waitInMills, failoverActive.booleanValue());
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

    private static String runPreAggregation(final Grid grid, final String origMapQuery, final String preAggrMapFp, final String preAggrReduceFp, final long waitInMills, final boolean failoverActive)
            throws CommandException {
        if(!origMapQuery.contains("<src>")) {
            throw new IllegalArgumentException("mapQuery does not need to perform pre-aggregation: "
                    + origMapQuery);
        }
        File mapFile = new File(preAggrMapFp);
        File reduceFile = new File(preAggrReduceFp);
        if(!mapFile.exists()) {
            throw new CommandException("Pre-aggregate map query file does not exist: "
                    + mapFile.getAbsolutePath());
        }
        if(!reduceFile.exists()) {
            throw new CommandException("Pre-aggregate reduce query file does not exist: "
                    + reduceFile.getAbsolutePath());
        }
        String mapQuery = FileUtils.toString(mapFile);
        String reduceQuery = FileUtils.toString(reduceFile);

        final ParallelSQLExecJob.JobConf jobConf = new ParallelSQLExecJob.JobConf(null, mapQuery, reduceQuery, OutputMethod.scalarString, waitInMills, failoverActive);
        final String replacement;
        try {
            replacement = grid.execute(ParallelSQLExecJob.class, jobConf);
        } catch (RemoteException e) {
            throw new CommandException(e);
        }
        String modifiedQuery = origMapQuery.replaceFirst("<src>", replacement);
        return modifiedQuery;
    }

    public String usage() {
        return constructHelp("Executes a parallel SQL job", "-mapQuery FILENAME -reduceQuery FILENAME -outputName NAME [-preAggrMap FILENAME -preAggrReduce FILENAME] [-outputMethod NAME] [-waitSPE SECONDS] execute sql");
    }

}
