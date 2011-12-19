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

import gridool.annotation.GridAnnotationProcessor;
import gridool.cache.GridLocalCacheManager;
import gridool.communication.GridCommunicationManager;
import gridool.db.catalog.DistributionCatalog;
import gridool.db.dba.DBAccessor;
import gridool.db.dba.DBAccessorFactory;
import gridool.db.partitioning.phihash.csv.distmm.InMemoryMappingIndex;
import gridool.dfs.GridXferService;
import gridool.dht.ILocalDirectory;
import gridool.discovery.GridDiscoveryService;
import gridool.locking.LockManager;
import gridool.locking.LockManagerFactory;
import gridool.marshaller.GridMarshaller;
import gridool.marshaller.GridMarshallerFactory;
import gridool.metrics.GridNodeMetricsService;
import gridool.metrics.runtime.GridTaskMetricsCounter;
import gridool.monitor.GridExecutionMonitor;
import gridool.processors.task.GridTaskProcessor;
import gridool.replication.ReplicationManager;
import gridool.routing.GridRouter;
import gridool.routing.GridRouterFactory;
import gridool.taskqueue.GridTaskQueueManager;
import gridool.util.GridUtils;
import gridool.util.primitive.Primitives;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridResourceRegistry {

    private final GridKernel kernel;
    private final Map<String, GridService> serviceRegistry;
    private final GridMarshaller marshaller;
    private final ConcurrentMap<String, URLClassLoader> ldrMap;
    private final AtomicReference<GridTaskMetricsCounter> taskMetricsCounter;
    private final LockManager lockManager;

    private GridNodeMetricsService metricsService;
    private GridNodeMetrics localNodeMetrics;
    private GridCommunicationManager communicationManager;
    private GridTaskQueueManager taskManager;
    private GridDiscoveryService discoveryService;
    private GridAnnotationProcessor annotationProc;
    private GridTaskProcessor taskProcessor;
    private ILocalDirectory directory;
    private GridExecutionMonitor executionMonitor;

    private final GridRouter router;
    private final DBAccessor dbAccessor;
    private final ReplicationManager replicationManager;
    private final DistributionCatalog distributionCatalog;

    private final GridLocalCacheManager localCache;
    private final InMemoryMappingIndex mappingIndex;

    private GridXferService dfsService;

    public GridResourceRegistry(@Nonnull GridKernel kernel, @Nonnull GridConfiguration config) {
        this.kernel = kernel;
        this.serviceRegistry = new HashMap<String, GridService>();
        this.marshaller = GridMarshallerFactory.createMarshaller();
        this.ldrMap = new ConcurrentHashMap<String, URLClassLoader>(32);
        GridTaskMetricsCounter counter = new GridTaskMetricsCounter();
        this.taskMetricsCounter = new AtomicReference<GridTaskMetricsCounter>(counter);
        this.lockManager = LockManagerFactory.createLockManager(config);
        this.dbAccessor = DBAccessorFactory.createDBAccessor();
        this.router = GridRouterFactory.createRouter(config);
        this.replicationManager = new ReplicationManager(kernel, dbAccessor, router, config);
        this.distributionCatalog = new DistributionCatalog(dbAccessor);
        this.localCache = new GridLocalCacheManager();
        int expectedEntries = Primitives.parseInt(Settings.get("grid.ld.mapping_index.expected_entries"), 10000);
        this.mappingIndex = new InMemoryMappingIndex(expectedEntries);
    }

    public GridKernel getGridKernel() {
        return kernel;
    }

    public GridService findService(String srvName) {
        return serviceRegistry.get(srvName);
    }

    public void registerService(GridService srv) {
        serviceRegistry.put(srv.getServiceName(), srv);
    }

    public Collection<GridService> getRegisteredServices() {
        return serviceRegistry.values();
    }

    public void setNodeMetricsService(@Nonnull GridNodeMetricsService metricsService) {
        this.metricsService = metricsService;
    }

    @CheckForNull
    public GridNodeMetricsService getNodeMetricsService() {
        if(metricsService == null) {
            throw new GridResourceNotFoundException("GridNodeMetricsUpdateService is not registered.");
        }
        return metricsService;
    }

    @Nonnull
    public GridRouter getRouter() throws GridResourceNotFoundException {
        return router;
    }

    @CheckForNull
    public GridCommunicationManager getCommunicationManager() {
        if(communicationManager == null) {
            throw new GridResourceNotFoundException("GridCommunicationManager is not registered.");
        }
        return communicationManager;
    }

    public void setCommunicationManager(GridCommunicationManager communicationManager) {
        this.communicationManager = communicationManager;
    }

    @CheckForNull
    public GridTaskQueueManager getTaskManager() {
        if(taskManager == null) {
            throw new GridResourceNotFoundException("GridTaskManager is not registered.");
        }
        return taskManager;
    }

    public void setTaskManager(@Nonnull GridTaskQueueManager taskManager) {
        this.taskManager = taskManager;
    }

    @CheckForNull
    public GridDiscoveryService getDiscoveryService() {
        if(discoveryService == null) {
            throw new GridResourceNotFoundException("GridDiscoveryService is not registered.");
        }
        return discoveryService;
    }

    public void setDiscoveryService(@Nonnull GridDiscoveryService discoveryService) {
        this.discoveryService = discoveryService;
    }

    public ILocalDirectory getDirectory() {
        return directory;
    }

    public void setDirectory(ILocalDirectory directory) {
        this.directory = directory;
    }

    public void setAnnotationProcessor(GridAnnotationProcessor proc) {
        this.annotationProc = proc;
    }

    public GridAnnotationProcessor getAnnotationProcessor() {
        return annotationProc;
    }

    public GridTaskProcessor getTaskProcessor() {
        return taskProcessor;
    }

    public void setTaskProcessor(GridTaskProcessor taskProcessor) {
        this.taskProcessor = taskProcessor;
    }

    public GridMarshaller getMarshaller() {
        return marshaller;
    }

    @Nonnull
    public ClassLoader getDeploymentGroupClassLoader(@Nullable String group) {
        if(group == null) {
            return Thread.currentThread().getContextClassLoader();
        }
        URLClassLoader ldr = ldrMap.get(group);
        if(ldr == null) {
            URL[] jarUrls = GridUtils.findJars(group);
            if(jarUrls == null) {
                return Thread.currentThread().getContextClassLoader();
            }
            ldr = new URLClassLoader(jarUrls);
            URLClassLoader prevLdr = ldrMap.putIfAbsent(group, ldr);
            if(prevLdr != null) {
                ldr = prevLdr;
            }
        }
        return ldr;
    }

    @Nonnull
    public ClassLoader getDeploymentGroupClassLoader(@Nullable String group, @Nonnull ClassLoader defaultLdr) {
        if(group == null) {
            return defaultLdr;
        }
        URLClassLoader ldr = ldrMap.get(group);
        if(ldr == null) {
            URL[] jarUrls = GridUtils.findJars(group);
            if(jarUrls == null) {
                return defaultLdr;
            }
            ldr = new URLClassLoader(jarUrls);
            URLClassLoader prevLdr = ldrMap.putIfAbsent(group, ldr);
            if(prevLdr != null) {
                ldr = prevLdr;
            }
        }
        return ldr;
    }

    @Nonnull
    public AtomicReference<GridTaskMetricsCounter> getTaskMetricsCounter() {
        return taskMetricsCounter;
    }

    public GridNodeMetrics getLocalNodeMetrics() {
        return localNodeMetrics;
    }

    public void setLocalNodeMetrics(@Nonnull GridNodeMetrics localMetrics) {
        this.localNodeMetrics = localMetrics;
    }

    public GridExecutionMonitor getExecutionMonitor() {
        return executionMonitor;
    }

    public void setExecutionMonitor(GridExecutionMonitor executionMonitor) {
        this.executionMonitor = executionMonitor;
    }

    @Nonnull
    public LockManager getLockManager() {
        return lockManager;
    }

    @Nonnull
    public DBAccessor getDbAccessor() {
        return dbAccessor;
    }

    @Nonnull
    public ReplicationManager getReplicationManager() {
        return replicationManager;
    }

    @Nonnull
    public DistributionCatalog getDistributionCatalog() {
        return distributionCatalog;
    }

    @Nonnull
    public GridLocalCacheManager getLocalCache() {
        return localCache;
    }

    @Nonnull
    public InMemoryMappingIndex getMappingIndex() {
        return mappingIndex;
    }

    public GridXferService getDfsService() {
        return dfsService;
    }

    public void setDfsService(GridXferService dfsService) {
        this.dfsService = dfsService;
    }

    private static final class GridResourceNotFoundException extends GridRuntimeException {
        private static final long serialVersionUID = 2531021671875008224L;

        public GridResourceNotFoundException(String msg) {
            super(GridErrorDescription.GRID_SERVICE_NOT_FOUND.message(msg));
        }

    }

}
