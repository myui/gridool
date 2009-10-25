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
package gridool.communication.transport.server;

import java.io.IOException;

import javax.annotation.Nonnull;

import gridool.GridConfiguration;
import gridool.GridException;
import gridool.communication.GridTransportListener;
import gridool.communication.transport.GridTransportServer;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridMinaServer implements GridTransportServer {

    public GridMinaServer(@Nonnull GridConfiguration config) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void start() throws GridException {}

    @Override
    public void close() throws IOException {}

    @Override
    public void setListener(GridTransportListener listener) {}

}
