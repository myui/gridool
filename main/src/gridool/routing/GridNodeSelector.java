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

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import gridool.GridConfiguration;
import gridool.GridNode;
import gridool.GridTask;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public interface GridNodeSelector {

    @Nullable
    GridNode selectNode(@Nonnull List<GridNode> nodeList, @Nullable GridTask task, @Nonnull GridConfiguration config);

    @Nonnull
    List<GridNode> sortNodes(@Nonnull List<GridNode> nodeList, @Nonnull byte[] key, @Nonnull GridConfiguration config);

}
