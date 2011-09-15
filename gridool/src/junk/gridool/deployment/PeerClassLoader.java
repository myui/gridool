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
package gridool.deployment;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridResourceRegistry;
import gridool.GridTaskResult;
import gridool.communication.GridCommunicationManager;
import gridool.taskqueue.sender.SenderResponseTaskQueue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Per-request class loader that delegates per-node class loader.
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class PeerClassLoader extends ClassLoader {
    private static final Log LOG = LogFactory.getLog(PeerClassLoader.class);

    @Nonnull
    private final GridPerNodeClassLoader parentLdr;
    @Nonnull
    private final GridNode remoteNode;
    @Nonnegative
    private final long timestamp;
    @Nonnull
    private final GridCommunicationManager communicator;
    @Nonnull
    private final SenderResponseTaskQueue responseQueue;

    public PeerClassLoader(GridPerNodeClassLoader parent, @Nonnull GridNode node, @Nonnegative long timestamp, @Nonnull GridResourceRegistry resourceRegistry) {
        super(parent);
        this.parentLdr = parent;
        this.remoteNode = node;
        this.timestamp = timestamp;
        this.communicator = resourceRegistry.getCommunicationManager();
        this.responseQueue = resourceRegistry.getTaskManager().getSenderResponseQueue();
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        final Class<?> onetimeClass = findLoadedClass(name);
        if(onetimeClass != null) {
            return onetimeClass;
        }
        final Long prevTimestamp = parentLdr.getTimestamp(name);
        if(prevTimestamp != null && prevTimestamp.longValue() < timestamp) {//newer version of class is found
            if(LOG.isInfoEnabled()) {
                LOG.info("Reloading a class '" + name + "' of timestamp '" + prevTimestamp
                        + "' with one of timestamp '" + timestamp);
            }
            return findClass(name);
        }
        Class<?> c;
        try {
            c = parentLdr.loadClass(name);
        } catch (ClassNotFoundException e) {
            // If still not found, then invoke findClass in order
            // to find the class.
            c = findClass(name);
        }
        return c;
    }

    @Override
    public Class<?> findClass(String name) throws ClassNotFoundException {
        BlockingQueue<GridTaskResult> resultQueue = new SynchronousQueue<GridTaskResult>();
        String jobId = "p2p-classloading_" + resultQueue.hashCode();
        responseQueue.addResponseQueue(jobId, resultQueue);

        if(LOG.isInfoEnabled()) {
            LOG.info("Loading a class '" + name + "' from " + remoteNode);
        }

        GridNode localNode = communicator.getLocalNode();
        final GridGetClassTask task = new GridGetClassTask(jobId, localNode, name);
        try {// send a class-loading request
            communicator.sendTaskRequest(task, remoteNode);
        } catch (GridException e) {
            throw new ClassNotFoundException("Failed sending a GridGetClassTask of the class: "
                    + name, e);
        }

        // Receive a requested class
        final GridTaskResult result;
        try {
            result = resultQueue.take(); // TODO timeout
        } catch (InterruptedException e) {
            throw new ClassNotFoundException("An error caused while receiving a class: " + name, e);
        }
        final ClassData clazz = result.getResult();
        assert (clazz != null);
        byte[] clazzData = clazz.getClassData();
        long ts = clazz.getTimestamp();
        final Class<?> definedClazz = parentLdr.defineClassIfNeeded(name, clazzData, ts);

        // define enclosing classes
        ClassData enclosingClass = clazz.getEnclosingClass();
        while(enclosingClass != null) {
            defineClass(enclosingClass, parentLdr);
            enclosingClass = enclosingClass.getEnclosingClass();
        }

        return definedClazz;
    }

    private void defineClass(@Nonnull ClassData clazz, @Nonnull GridPerNodeClassLoader ldr) {
        String clsName = clazz.getClassName();
        byte[] b = clazz.getClassData();
        long timestamp = clazz.getTimestamp();
        if(ldr.defineClassIfNeeded(clsName, b, timestamp) == null) {
            defineClass(clsName, b, 0, b.length);
        }
    }

}
