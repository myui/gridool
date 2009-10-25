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
package gridool;

import gridool.communication.GridCommunicationManager;
import gridool.deployment.GridPerNodeClassLoader;
import gridool.routing.GridNodeSelector;
import gridool.routing.GridNodeSelectorFactory;
import gridool.routing.GridTaskRouter;
import gridool.util.GridUtils;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.config.Settings;
import xbird.engine.InternalException;
import xbird.engine.RemoteBase;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridServer extends RemoteBase implements Grid {
    private static final long serialVersionUID = 7051581765893315286L;
    private static final Log LOG = LogFactory.getLog(GridServer.class);

    public static final String bindName = Settings.get("gridool.server.name");
    private static final int exportPort = Integer.parseInt(Settings.get("gridool.server.port", "0"));

    private final GridKernel kernel;

    public GridServer() {
        super(bindName, exportPort);
        this.kernel = GridFactory.makeGrid();
    }

    @Nonnull
    public GridResourceRegistry getResourceRegistry() {
        return kernel.getResourceRegistry();
    }

    @Override
    public void start() throws InternalException {
        try {
            kernel.start();
        } catch (GridException e) {
            throw new InternalException("Failed to start a grid", e);
        }
        super.start();
    }

    @Override
    public void shutdown(boolean forceExit) throws RemoteException {
        kernel.stop(forceExit);
        super.shutdown(forceExit);
    }

    public void deployClass(String clsName, byte[] clazz, long timestamp) throws RemoteException {
        try {
            kernel.deployJob(clsName, clazz, timestamp);
        } catch (GridException e) {
            LOG.error(e.getMessage(), e);
            throw new RemoteException(e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    public <A extends Serializable, R extends Serializable> R execute(String jobClassName, A arg)
            throws RemoteException {
        GridResourceRegistry registry = kernel.getResourceRegistry();
        GridCommunicationManager communicator = registry.getCommunicationManager();
        GridNode localNode = communicator.getLocalNodeInfo();
        GridPerNodeClassLoader ldr = registry.getNodeClassLoader(localNode);
        final Class<? extends GridJob<A, R>> jobClass;
        try {
            jobClass = (Class<? extends GridJob<A, R>>) Class.forName(jobClassName, true, ldr);
        } catch (ClassNotFoundException e) {
            throw new RemoteException("Class not found: " + jobClassName, e);
        }
        return execute(jobClass, arg);
    }

    public <A extends Serializable, R extends Serializable> R execute(Class<? extends GridJob<A, R>> jobClass, A arg)
            throws RemoteException {
        final GridJobFuture<R> future = kernel.execute(jobClass, arg);
        try {
            return future.get();
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
            throw new RemoteException(e.getMessage(), e);
        } catch (ExecutionException e) {
            LOG.error(e.getMessage(), e);
            throw new RemoteException(e.getMessage(), e);
        }
    }

    public GridNode delegate(boolean onlySuperNode) throws RemoteException {
        final GridTaskRouter router = kernel.getResourceRegistry().getTaskRouter();
        final GridNode[] nodes = router.getAllNodes();
        if(nodes.length == 0) {
            if(LOG.isInfoEnabled()) {
                LOG.info("No node found on the grid");
            }
            return null;
        }

        final List<GridNode> nodeList;
        if(onlySuperNode) {
            final List<GridNode> superNodes = GridUtils.selectSuperNodes(nodes);
            if(superNodes.isEmpty()) {
                final String errmsg = "Super nodes do not exist in the Grid (total " + nodes.length
                        + " nodes)";
                LOG.error(errmsg);
                throw new RemoteException(errmsg);
            } else {
                nodeList = superNodes;
            }
        } else {
            nodeList = Arrays.asList(nodes);
        }

        GridNodeSelector selector = GridNodeSelectorFactory.createSelector();
        GridConfiguration config = kernel.getConfiguration();
        GridNode node = selector.selectNode(nodeList, null, config);
        return node;
    }

}
