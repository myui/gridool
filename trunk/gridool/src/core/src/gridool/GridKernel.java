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

import gridool.communication.CommunicationServiceProvider;
import gridool.communication.GridCommunicationManager;
import gridool.communication.GridCommunicationService;
import gridool.db.GridDatabaseService;
import gridool.dfs.GridXferService;
import gridool.dht.DirectoryService;
import gridool.discovery.DiscoveryServiceProvider;
import gridool.discovery.GridDiscoveryService;
import gridool.memcached.gateway.MemcachedGateway;
import gridool.memcached.server.MemcachedServer;
import gridool.metrics.GridNodeMetricsService;
import gridool.monitor.GridExecutionMonitor;
import gridool.monitor.GridMonitorFactory;
import gridool.processors.GridProcessorProvider;
import gridool.processors.job.GridJobProcessor;
import gridool.processors.task.GridTaskProcessorService;
import gridool.replication.ReplicationService;
import gridool.taskqueue.GridTaskQueueManager;
import gridool.tools.GridDeadlockDetectionService;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridKernel {
    private static final Log LOG = LogFactory.getLog(GridKernel.class);

    @Nonnull
    private final GridConfiguration config;
    @Nonnull
    private final GridResourceRegistry resourceRegistry;

    private GridJobProcessor jobProcessor;

    public GridKernel(@CheckForNull GridConfiguration config) {
        if(config == null) {
            throw new IllegalArgumentException();
        }
        this.config = config;
        this.resourceRegistry = new GridResourceRegistry(this, config);
    }

    public GridConfiguration getConfiguration() {
        return config;
    }

    public GridResourceRegistry getResourceRegistry() {
        return resourceRegistry;
    }

    public void start() throws GridException {
        initializeResources();
        LOG.info("Grid started");
    }

    private void initializeResources() throws GridException {
        GridNodeMetricsService metricsServ = new GridNodeMetricsService(resourceRegistry, config);
        GridDiscoveryService discoveryServ = DiscoveryServiceProvider.createService(resourceRegistry, config);

        if(!config.isJoinToMembership()) {
            registerServices(metricsServ, discoveryServ);
            return;
        }

        GridTaskQueueManager taskManager = new GridTaskQueueManager(resourceRegistry);
        GridCommunicationService communicationServ = CommunicationServiceProvider.createService(taskManager, resourceRegistry, config);
        GridCommunicationManager communicationMgr = new GridCommunicationManager(resourceRegistry, communicationServ);

        GridExecutionMonitor monitor = GridMonitorFactory.createExecutionMonitor(resourceRegistry);

        GridTaskProcessorService taskProcServ = GridProcessorProvider.createTaskProcessorService(communicationMgr, monitor, resourceRegistry, config);
        DirectoryService dirServ = new DirectoryService(config, resourceRegistry);
        GridXferService dfsServ = new GridXferService(config, resourceRegistry);
        GridDeadlockDetectionService deadlockServ = new GridDeadlockDetectionService();

        registerServices(metricsServ, discoveryServ, communicationServ, taskProcServ, dirServ, dfsServ, deadlockServ);

        // DB services
        if(config.isDbFeatureEnabled()) {
            ReplicationService replServ = new ReplicationService(resourceRegistry);
            GridDatabaseService databaseServ = new GridDatabaseService(resourceRegistry);
            try {
                registerServices(replServ, databaseServ);
            } catch (GridException e) {
                if(config.isAutoDetectDB()) {
                    LOG.info("Database service is disabled");
                    config.setDbFeatureEnabled(false);
                } else {
                    throw e;
                }
            }
        }

        // Memcached
        if(Boolean.parseBoolean(Settings.get("gridool.memcached.feature_enabled"))) {
            MemcachedGateway memcachedGateway = new MemcachedGateway(resourceRegistry);
            MemcachedServer memcachedServer = new MemcachedServer(resourceRegistry);
            registerServices(memcachedGateway, memcachedServer);
        }

        this.jobProcessor = new GridJobProcessor(monitor, resourceRegistry, config);
    }

    public void registerServices(@Nonnull GridService... services) throws GridException {
        for(GridService srv : services) {
            if(srv != null) {
                srv.start();
                resourceRegistry.registerService(srv);
            }
        }
    }

    public void stop() {
        stop(true);
    }

    public void stop(boolean cancel) {
        try {
            if(config.isJoinToMembership()) {
                stopProcessors(cancel);
            }
            stopServices();
        } catch (GridException e) {
            LOG.error(e);
        }
        LOG.info("Grid stopped");
    }

    private void stopProcessors(boolean cancel) throws GridException {
        jobProcessor.stop(cancel);
    }

    private void stopServices() throws GridException {
        for(GridService service : resourceRegistry.getRegisteredServices()) {
            service.stop();
        }
    }

    public <A, R> GridJobFuture<R> execute(@Nonnull Class<? extends GridJob<A, R>> jobClass, @Nullable A arg) {
        return execute(jobClass, arg, null);
    }

    public <A, R> GridJobFuture<R> execute(@Nonnull Class<? extends GridJob<A, R>> jobClass, @Nullable A arg, @Nullable String deploymentGroup) {
        return jobProcessor.execute(jobClass, arg, deploymentGroup);
    }

}
