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
package gridool.loadblancing.probe;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import gridool.GridNode;
import gridool.GridNodeMetrics;
import gridool.loadblancing.GridLoadProbe;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridCpuLoadProbe implements GridLoadProbe {

    private static final float DEFAULT_LOAD = 0.4f;

    public GridCpuLoadProbe() {}

    @Nonnegative
    public float getLoad(@Nonnull GridNode node) {
        final GridNodeMetrics metrics = node.getMetrics();
        if(metrics == null) {
            return DEFAULT_LOAD;
        }
        return metrics.getLastCpuLoad();
    }

}
