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
package gridool.dht;

import gridool.GridException;
import gridool.GridKernel;
import gridool.GridResourceRegistry;
import gridool.GridService;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class DHTService implements GridService {

    private final DHTServer server;

    public DHTService(GridKernel grid, GridResourceRegistry registry) {
        registry.setDhtService(this);
        this.server = new DHTServer();
    }

    public DHTServer getServer() {
        return server;
    }

    @Override
    public String getServiceName() {
        return DHTService.class.getName();
    }

    @Override
    public void start() throws GridException {
    // nop
    }

    @Override
    public void stop() throws GridException {
        server.close();
    }

}
