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
import gridool.replication.jobs.CoordinateReplicaJob;
import gridool.replication.jobs.CoordinateReplicaJobConf;
import gridool.util.cmdline.CommandBase;
import gridool.util.cmdline.CommandException;
import gridool.util.cmdline.Option.StringOption;

import java.rmi.RemoteException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * coordinate replica NUMBER reorg?
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class CoordinateReplicaCommand extends CommandBase {
    private static final Log LOG = LogFactory.getLog(CoordinateReplicaCommand.class);

    public CoordinateReplicaCommand() {
        super();
        addOption(new StringOption("driverClassName", "nl.cwi.monetdb.jdbc.MonetDriver", true));
        addOption(new StringOption("primaryDbUrl", true));
        addOption(new StringOption("user", true));
        addOption(new StringOption("passwd", true));
    }

    public boolean match(String[] args) {
        final int arglen = args.length;
        if(arglen < 3 || arglen > 4) {
            return false;
        }
        if(arglen == 4) {
            if(!"reorg".equalsIgnoreCase(args[3])) {
                return false;
            }
        }
        if(!"coordinate".equalsIgnoreCase(args[0])) {
            return false;
        }
        if(!"replica".equalsIgnoreCase(args[1])) {
            return false;
        }
        final int replicas;
        try {
            replicas = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            return false;
        }
        if(replicas < 1) {
            return false;
        }
        return true;
    }

    public boolean process(String[] args) throws CommandException {
        String driverClassName = getOption("driverClassName");
        String primaryDbUrl = getOption("primaryDbUrl");
        String user = getOption("user");
        String passwd = getOption("passwd");

        boolean reorg = (args.length == 4);
        int numReplicas = Integer.parseInt(args[2]);
        final CoordinateReplicaJobConf jobConf = new CoordinateReplicaJobConf(driverClassName, primaryDbUrl, user, passwd, numReplicas, reorg);

        final Grid grid = new GridClient();
        final GridNode[] failedNodes;
        try {
            failedNodes = grid.execute(CoordinateReplicaJob.class, jobConf);
        } catch (RemoteException e) {
            throwException(e.getMessage());
            return false;
        }
        if(failedNodes.length > 0) {
            LOG.warn("Could not coordinate replicas properly. failed on "
                    + Arrays.toString(failedNodes));
            return false;
        }
        LOG.info("Coordinated replicas successfully.");
        return true;
    }

    public String usage() {
        return constructHelp("coordinate replicas in the cluster", "coordinate replica NUMBER reorg?");
    }

}
