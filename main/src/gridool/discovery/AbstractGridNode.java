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
package gridool.discovery;

import java.net.InetAddress;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import gridool.GridNode;
import gridool.GridNodeMetrics;
import gridool.communication.payload.GridNodeInfo;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public abstract class AbstractGridNode extends GridNodeInfo {
    private static final long serialVersionUID = -6266288479721721069L;

    private transient GridNodeMetrics metrics;

    public AbstractGridNode() {
        super();
    } // for Externalizable

    public AbstractGridNode(@CheckForNull InetAddress addr, int port, boolean superNode, @Nullable GridNodeMetrics metrics) {
        super(addr, port, superNode);
        this.metrics = metrics;
    }

    public AbstractGridNode(GridNode nodeInfo, GridNodeMetrics metrics) {
        super(nodeInfo);
        this.metrics = metrics;
    }

    @Override
    public final GridNodeMetrics getMetrics() {
        return metrics;
    }

    @Override
    public final void setMetrics(@Nonnull GridNodeMetrics metrics) {
        if(metrics == null) {
            throw new IllegalArgumentException();
        }
        this.metrics = metrics;
    }

}
