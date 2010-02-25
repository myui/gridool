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
package gridool.tools;

import gridool.tools.cmd.CoordinateReplicaCommand;
import gridool.tools.cmd.ParallelSQLExecCommand;
import gridool.tools.cmd.RegisterPartitionedTableCommand;
import gridool.tools.cmd.RegisterReplicaCommand;
import xbird.util.cmdline.Command;
import xbird.util.cmdline.CommandInvokerBase;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridCommandInvoker extends CommandInvokerBase {

    public GridCommandInvoker() {
        super(listCommands());
    }

    private static Command[] listCommands() {
        return new Command[] { new RegisterReplicaCommand(), new CoordinateReplicaCommand(),
                new ParallelSQLExecCommand(), new RegisterPartitionedTableCommand() };
    }

    public static void main(String[] args) {
        new GridCommandInvoker().run(args);
    }

}
