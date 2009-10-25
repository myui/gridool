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
package gridool.routing;

import gridool.GridNode;
import gridool.GridTask;
import gridool.discovery.GridDiscoveryListener;

import java.util.List;

import javax.annotation.Nonnull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public interface GridTaskRouter extends GridDiscoveryListener {

    GridNodeSelector getNodeSelector();
    
    int getGridSize();    

    @Nonnull
    GridNode[] getAllNodes();

    @Nonnull
    List<GridNode> getNodes(int maxNodesToSelect);

    @Nonnull
    GridNode selectNode(@Nonnull GridTask task);

    @Nonnull
    List<GridNode> selectNodes(@Nonnull byte[] key);

    @Nonnull
    List<GridNode> selectNodes(@Nonnull byte[] key, int maxNodesToSelect);

}
