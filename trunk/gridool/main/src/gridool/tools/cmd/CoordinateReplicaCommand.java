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
import gridool.replication.jobs.CoordinateReplicaJob;

import java.rmi.RemoteException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.util.cmdline.CommandBase;
import xbird.util.cmdline.CommandException;

/**
 * coordinate replica NUMBER reorg?
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class CoordinateReplicaCommand extends CommandBase {
    private static final Log LOG = LogFactory.getLog(CoordinateReplicaCommand.class);

    public CoordinateReplicaCommand() {}

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
        boolean reorg = "reorg".equalsIgnoreCase(args[3]);
        int numReplicas = Integer.parseInt(args[2]);
        final CoordinateReplicaJob.JobConf jobConf = new CoordinateReplicaJob.JobConf(numReplicas, reorg);

        final Grid grid = new GridClient();
        final int minReplicas;
        try {
            minReplicas = grid.execute(CoordinateReplicaJob.class, jobConf);
        } catch (RemoteException e) {
            throwException(e.getMessage());
            return false;
        }
        if(minReplicas < 1) {
            LOG.warn("Could not coordinate replicas properly. minReplica = " + minReplicas);
            return false;
        }
        LOG.info("Coordinated replicas successfully. minReplica = " + minReplicas);
        return true;
    }

    public String usage() {
        return constructHelp("coordinate replicas in the cluster", "coordinate replica NUMBER reorg?");
    }

}
