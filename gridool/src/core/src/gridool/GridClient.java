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

import gridool.util.GridUtils;
import gridool.util.net.NetUtils;
import gridool.util.remoting.RemoteBase;

import java.io.Serializable;
import java.rmi.Naming;
import java.rmi.RemoteException;

import javax.annotation.Nonnull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridClient implements Grid {

    private final String remoteEndpoint;
    private Grid gridRef = null;

    public GridClient(String remoteEndpoint) {
        if(remoteEndpoint == null) {
            this.remoteEndpoint = "//" + NetUtils.getLocalHostAddressAsUrlString() + ":"
                    + RemoteBase.localRegistryPort + '/' + GridServer.bindName;
        } else {
            this.remoteEndpoint = remoteEndpoint;
        }
    }

    public GridClient() {
        this((String) null);
    }

    public GridClient(@Nonnull GridNode node) {
        this(GridUtils.getGridEndpoint(node));
    }

    public String getEndpoint() {
        return remoteEndpoint;
    }

    private synchronized void prepare() {
        if(gridRef == null) {
            try {
                this.gridRef = (Grid) Naming.lookup(remoteEndpoint);
            } catch (Exception e) {
                throw new IllegalStateException("Look up failed: " + remoteEndpoint, e);
            }
        }
    }

    @Override
    public <A extends Serializable, R extends Serializable> R execute(Class<? extends GridJob<A, R>> jobClass, A arg)
            throws RemoteException {
        GridJobDesc jobDesc = new GridJobDesc(jobClass);
        return execute(jobDesc, arg);
    }

    @Override
    public <A extends Serializable, R extends Serializable> R execute(GridJobDesc jobDesc, A arg)
            throws RemoteException {
        prepare();
        return gridRef.<A, R> execute(jobDesc, arg);
    }

    @Override
    public GridNode delegate(boolean onlySuperNode) throws RemoteException {
        prepare();
        GridNode node = gridRef.delegate(onlySuperNode);
        return node;
    }

}
