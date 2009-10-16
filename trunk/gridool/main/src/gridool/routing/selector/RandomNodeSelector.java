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
package gridool.routing.selector;

import java.util.Collections;
import java.util.List;
import java.util.Random;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import gridool.GridConfiguration;
import gridool.GridNode;
import gridool.GridTask;
import gridool.routing.GridNodeSelector;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class RandomNodeSelector implements GridNodeSelector {

    @Nonnull
    private final Random rand = new Random();

    public RandomNodeSelector() {}

    @Nullable
    public GridNode selectNode(@Nonnull List<GridNode> nodeList, @Nullable GridTask task, GridConfiguration config) {
        final int size = nodeList.size();
        if(size == 0) {
            return null;
        } else if(size == 1) {
            return nodeList.get(0);
        } else {
            int index = rand.nextInt(size);
            GridNode node = nodeList.get(index);
            return node;
        }
    }

    public List<GridNode> sortNodes(@Nonnull List<GridNode> nodeList, byte[] key, GridConfiguration config) {
        Collections.shuffle(nodeList, rand);
        return nodeList;
    }

}
