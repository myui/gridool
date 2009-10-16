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
package gridool.connector;

import java.util.Random;

import javax.annotation.Nonnull;

import gridool.Grid;
import gridool.GridClient;
import gridool.GridConfiguration;
import gridool.GridConnectionBroker;
import gridool.GridException;
import gridool.GridFactory;
import gridool.GridKernel;
import gridool.GridNode;
import gridool.routing.GridTaskRouter;
import gridool.util.GridUtils;
import xbird.config.Settings;
import xbird.util.lang.Primitives;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridRandomConnectionBroker implements GridConnectionBroker {

    private static final int INIT_DELAY = Primitives.parseInt(Settings.get("gridool.connector.init_delay"), 2000);

    public GridRandomConnectionBroker() {}

    public Grid connect() throws GridException {
        GridNode[] nodes = availableNodes();
        if(nodes.length == 0) {
            return new GridClient();
        }
        GridNode node = selectNode(nodes);

        String endpoint = GridUtils.getGridEndpoint(node);
        Grid grid = new GridClient(endpoint);
        return grid;
    }

    @Nonnull
    private static GridNode selectNode(GridNode[] nodes) throws GridException {
        final int size = nodes.length;
        Random rand = new Random();
        int n = rand.nextInt(size);
        return nodes[n];
    }

    @Nonnull
    private static GridNode[] availableNodes() throws GridException {
        final GridConfiguration config = new GridConfiguration();
        config.setJoinToMembership(false);
        final GridKernel grid = GridFactory.makeGrid(config);
        try {
            grid.start();
            if(INIT_DELAY > 0) {
                try {
                    Thread.sleep(INIT_DELAY);
                } catch (InterruptedException e) {
                    throw new GridException(e);
                }
            }
            GridTaskRouter router = grid.getResourceRegistry().getTaskRouter();
            return router.getAllNodes();
        } finally {
            grid.stop();
        }
    }

}
