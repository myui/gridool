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
package gridool.annotation;

import gridool.GridConfiguration;
import gridool.GridException;
import gridool.GridJob;
import gridool.GridKernel;
import gridool.GridResourceRegistry;
import gridool.GridTask;
import gridool.db.partitioning.csv.distmm.InMemoryMappingIndex;
import gridool.directory.ILocalDirectory;
import gridool.monitor.GridExecutionMonitor;
import gridool.util.annotation.AnnotationUtils;
import gridool.util.annotation.BasicResourceInjector;

import javax.annotation.Nonnull;

import org.apache.commons.logging.LogFactory;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridAnnotationProcessor {

    private final GridConfiguration config;
    private final GridResourceRegistry resourceRegistry;

    public GridAnnotationProcessor(@Nonnull GridConfiguration config, @Nonnull GridResourceRegistry registry) {
        this.config = config;
        this.resourceRegistry = registry;
        registry.setAnnotationProcessor(this);
    }

    @Nonnull
    public GridConfiguration getConfiguration() {
        return config;
    }

    @Nonnull
    public GridResourceRegistry getResourceRegistory() {
        return resourceRegistry;
    }

    public void injectResources(@Nonnull GridTask task) throws GridException {
        injectResouece(task, resourceRegistry, config);
    }

    @SuppressWarnings("unchecked")
    public void injectResources(@Nonnull GridJob job) throws GridException {
        injectResouece(job, resourceRegistry, config);
    }

    private static void injectResouece(final Object obj, final GridResourceRegistry resourceRegistry, final GridConfiguration config)
            throws GridException {
        final GridKernel kernel = resourceRegistry.getGridKernel();
        final ILocalDirectory directory = resourceRegistry.getDirectory();
        final GridExecutionMonitor monitor = resourceRegistry.getExecutionMonitor();
        final InMemoryMappingIndex mappingIndex = resourceRegistry.getMappingIndex();
        final BasicResourceInjector injector = new BasicResourceInjector(kernel);
        try {
            AnnotationUtils.injectFieldResource(obj, GridKernelResource.class, injector);
            injector.setResource(config);
            AnnotationUtils.injectFieldResource(obj, GridConfigResource.class, injector);
            injector.setResource(directory);
            AnnotationUtils.injectFieldResource(obj, GridDirectoryResource.class, injector);
            injector.setResource(monitor);
            AnnotationUtils.injectFieldResource(obj, GridExecutionMonitorResource.class, injector);
            injector.setResource(resourceRegistry);
            AnnotationUtils.injectFieldResource(obj, GridRegistryResource.class, injector);
            injector.setResource(mappingIndex);
            AnnotationUtils.injectFieldResource(obj, GridMappingIndexResource.class, injector);
        } catch (IllegalArgumentException arge) {
            String errmsg = "Failed to inject resources for " + obj;
            LogFactory.getLog(GridAnnotationProcessor.class).error(errmsg, arge);
            throw new GridException(errmsg, arge);
        } catch (IllegalAccessException acce) {
            String errmsg = "Failed to inject resources for " + obj;
            LogFactory.getLog(GridAnnotationProcessor.class).error(errmsg, acce);
            throw new GridException(errmsg, acce);
        }
    }

}
