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
import gridool.db.catalog.RegisterPartitionedTableJob;
import gridool.util.cmdline.CommandBase;
import gridool.util.cmdline.CommandException;
import gridool.util.cmdline.Option.StringOption;
import gridool.util.lang.ArrayUtils;
import gridool.util.struct.Pair;

import java.rmi.RemoteException;


/**
 * register partitioned table <tableName>+
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class RegisterPartitionedTableCommand extends CommandBase {

    public RegisterPartitionedTableCommand() {
        addOption(new StringOption("tpltblPrefix", true));
    }

    public boolean match(String[] args) {
        if(args.length < 4) {
            return false;
        }
        if(!"register".equalsIgnoreCase(args[0])) {
            return false;
        }
        if(!"partitioned".equalsIgnoreCase(args[1])) {
            return false;
        }
        if(!"table".equalsIgnoreCase(args[2])) {
            return false;
        }
        return true;
    }

    public boolean process(String[] args) throws CommandException {
        final String[] tableNames = ArrayUtils.copyOfRange(args, 3, args.length);
        final String templateTableNamePredix = getOption("tpltblPrefix");

        final Pair<String[], String> jobParams = new Pair<String[], String>(tableNames, templateTableNamePredix);
        final Grid grid = new GridClient();
        final int[] partitionIds;
        try {
            partitionIds = grid.execute(RegisterPartitionedTableJob.class, jobParams);
        } catch (RemoteException e) {
            throw new CommandException(e);
        }
        for(int i = 0; i < tableNames.length; i++) {
            System.out.println("Table: " + tableNames[i] + ", PartitionNo: " + partitionIds[i]);
        }
        return true;
    }

    public String usage() {
        return constructHelp("Register partitioned tables", "register partitioned table <tableName>+");
    }

}
