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
package sample;

import gridool.GridClient;
import gridool.GridException;
import gridool.GridJob;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskRelocatability;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.construct.GridJobBase;
import gridool.construct.GridTaskAdapter;
import gridool.routing.GridTaskRouter;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.IdentityHashMap;
import java.util.Map;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.ExampleMode;
import org.kohsuke.args4j.Option;

import xbird.util.struct.Pair;

/**
 * Run with an argument "-h" to show help. 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class WorkStealingSample {

    @Option(name = "-sleep", usage = "specify sleep time in msec (default: 10000)")
    private int sleepTime = 10000;

    @Option(name = "-times", usage = "specify tasks per node (default: 40)")
    private int tasksPerNode = 40;

    @Option(name = "-h", usage = "Show help (default=false)")
    private boolean _showHelp = false;

    public WorkStealingSample() {}

    public static void main(String[] args) throws RemoteException {
        new WorkStealingSample().doMain(args);
    }

    private void doMain(String[] args) throws RemoteException {
        final CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            showHelp(parser);
            return;
        }
        if(_showHelp) {
            showHelp(parser);
            return;
        }
        GridClient grid = new GridClient();
        //grid.deployClass(SleepJob.class);
        //grid.deployClass(SleepTask.class);
        grid.execute(SleepJob.class, new Pair<Integer, Integer>(sleepTime, tasksPerNode));
    }

    private static void showHelp(CmdLineParser parser) {
        System.err.println("[Usage] \n $ java " + WorkStealingSample.class.getSimpleName()
                + parser.printExample(ExampleMode.ALL));
        parser.printUsage(System.err);
    }

    public static final class SleepJob extends GridJobBase<Pair<Integer, Integer>, Serializable> {
        private static final long serialVersionUID = 1L;

        public SleepJob() {
            super();
        }

        public Map<GridTask, GridNode> map(GridTaskRouter router, Pair<Integer, Integer> args)
                throws GridException {
            final GridNode[] nodes = router.getAllNodes();
            final int sleep = args.getFirst().intValue();
            final int tasksPerNode = args.getSecond().intValue();
            final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>(nodes.length);
            for(GridNode node : nodes) {
                for(int i = 0; i < tasksPerNode; i++) {
                    map.put(new SleepTask(this, sleep), node);
                }
            }
            return map;
        }

        public GridTaskResultPolicy result(GridTaskResult result)
                throws GridException {
            return GridTaskResultPolicy.CONTINUE;
        }

        public Serializable reduce() throws GridException {
            return null;
        }
    }

    public static final class SleepTask extends GridTaskAdapter {
        private static final long serialVersionUID = 1L;

        private final int sleep;

        @SuppressWarnings("unchecked")
        public SleepTask(GridJob job, int sleep) {
            super(job, true);
            this.sleep = sleep;
        }

        @Override
        public GridTaskRelocatability getRelocatability() {
            return GridTaskRelocatability.relocatableToAnywhere;
        }

        @Override
        public boolean cancel() throws GridException {
            return super.cancel();
        }

        protected Serializable execute() throws GridException {
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                ;
            }
            return null;
        }

    }

}
