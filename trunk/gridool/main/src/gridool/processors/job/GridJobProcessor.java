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
package gridool.processors.job;

import gridool.GridConfiguration;
import gridool.GridException;
import gridool.GridJob;
import gridool.GridJobFuture;
import gridool.GridResourceRegistry;
import gridool.monitor.GridExecutionMonitor;
import gridool.processors.AbstractGridProcessor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.util.concurrent.ExecutorFactory;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridJobProcessor extends AbstractGridProcessor {
    private static final Log LOG = LogFactory.getLog(GridJobProcessor.class);

    @Nonnull 
    private final GridExecutionMonitor monitor;
    @Nonnull 
    private final ExecutorService execPool;

    public GridJobProcessor(@Nonnull GridExecutionMonitor monitor, @Nonnull GridResourceRegistry resourceRegistry, @Nonnull GridConfiguration config) {
        super(resourceRegistry, config);
        this.monitor = monitor;
        int poolSize = config.getJobProcessorPoolSize();
        this.execPool = ExecutorFactory.newFixedThreadPool(poolSize, "GridJobWorker");
    }

    public void stop(boolean cancel) throws GridException {
        if(cancel) {
            execPool.shutdownNow();
        } else {
            execPool.shutdown();
        }
    }

    public <A, R> GridJobFuture<R> execute(Class<? extends GridJob<A, R>> jobClass, A arg) {
        final GridJob<A, R> job;
        try {
            job = newJobInstance(jobClass);
        } catch (GridException e) {
            return new GridJobFutureAdapter<R>(e);
        }

        GridJobWorker<A, R> worker = new GridJobWorker<A, R>(job, arg, monitor, resourceRegistry, config);
        final FutureTask<R> future = worker.newTask();
        try {
            execPool.execute(future);
        } catch (Throwable e) {
            return new GridJobFutureAdapter<R>(e);
        }
        return new GridJobFutureAdapter<R>(future);
    }

    private static <A, R> GridJob<A, R> newJobInstance(Class<? extends GridJob<A, R>> jobClass)
            throws GridException {
        try {
            return jobClass.newInstance();
        } catch (InstantiationException e) {
            String errmsg = "Failed to instantiate a job class: " + jobClass;
            LOG.error(errmsg);
            throw new GridException(errmsg, e);
        } catch (IllegalAccessException iae) {
            String errmsg = "Failed to instantiate a job class: " + jobClass;
            LOG.error(errmsg);
            throw new GridException(errmsg, iae);
        }
    }

}
