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
import gridool.GridResourceRegistry;
import gridool.annotation.GridRegistryResource;
import gridool.construct.GridTaskAdapter;

import java.io.Serializable;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class GridDeployClassTask extends GridTaskAdapter {
    private static final long serialVersionUID = -2221741265392756567L;

    private final String clsName;
    private final byte[] clsBytes;
    private final long timestamp;

    @GridRegistryResource
    private transient GridResourceRegistry registry;

    public GridDeployClassTask(@Nonnull GridDeploymentJob job, @Nonnull String clsName, @Nonnull byte[] clsBytes, @Nonnegative long timestamp) {
        super(job, false);
        this.clsName = clsName;
        this.clsBytes = clsBytes;
        this.timestamp = timestamp;
    }

    @Override
    public boolean injectResources() {
        return true;
    }

    public Serializable execute() throws GridException {
        assert (senderNode != null);
        GridPerNodeClassLoader ldr = registry.getNodeClassLoader(senderNode);
        ldr.defineClassIfNeeded(clsName, clsBytes, timestamp);
        return null;
    }

}
