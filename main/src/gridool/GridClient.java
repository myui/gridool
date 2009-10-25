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

import java.io.IOException;
import java.io.Serializable;
import java.rmi.Naming;
import java.rmi.RemoteException;

import javax.annotation.Nonnull;

import xbird.engine.RemoteBase;
import xbird.util.lang.ClassUtils;
import xbird.util.net.NetUtils;

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

    /**
     * Note that method local classes are not deployed.
     * Both static and normal inner classes are deployed when <code>includeInnerClasses</code> is true.
     */
    @Deprecated
    public void deployJob(@Nonnull Class<? extends GridJob<?, ?>> jobClass, boolean includeInnerClasses)
            throws RemoteException {
        deployClass(jobClass, includeInnerClasses);
    }

    public void deployClass(@Nonnull Class<?> clazz) throws RemoteException {
        deployClass(clazz, true);
    }

    public void deployClass(@Nonnull Class<?> clazz, boolean includeInnerClasses)
            throws RemoteException {
        final String clsName = clazz.getName();
        final byte[] b;
        try {
            b = ClassUtils.getClassAsBytes(clazz);
        } catch (IOException e) {
            throw new RemoteException("Failed to get resouce: " + clsName, e);
        }
        final long timestamp = ClassUtils.getLastModified(clazz);
        deployClass(clsName, b, timestamp);

        if(includeInnerClasses) {
            final Class<?>[] innerClazz = clazz.getDeclaredClasses();
            for(Class<?> c : innerClazz) {
                deployInnerClasses(c, timestamp);
            }
        }
    }

    private void deployInnerClasses(final Class<?> clazz, final long timestamp)
            throws RemoteException {
        final String clsName = clazz.getName();
        final byte[] b;
        try {
            b = ClassUtils.getClassAsBytes(clazz);
        } catch (IOException e) {
            throw new RemoteException("Failed to get resouce: " + clsName, e);
        }
        deployClass(clsName, b, timestamp);

        final Class<?>[] innerClazz = clazz.getDeclaredClasses();
        for(Class<?> c : innerClazz) {
            deployInnerClasses(c, timestamp);
        }
    }

    public void deployClass(String clsName, byte[] clazz, long timestamp) throws RemoteException {
        prepare();
        gridRef.deployClass(clsName, clazz, timestamp);
    }

    public <A extends Serializable, R extends Serializable> R execute(String jobClassName, A arg)
            throws RemoteException {
        prepare();
        return gridRef.execute(jobClassName, arg);
    }

    public <A extends Serializable, R extends Serializable> R execute(Class<? extends GridJob<A, R>> jobClass, A arg)
            throws RemoteException {
        prepare();
        return gridRef.execute(jobClass, arg);
    }

    public GridNode delegate(boolean onlySuperNode) throws RemoteException {
        prepare();
        GridNode node = gridRef.delegate(onlySuperNode);
        return node;
    }

}
