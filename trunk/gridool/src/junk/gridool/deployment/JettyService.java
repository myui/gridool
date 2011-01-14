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
import gridool.GridService;
import gridool.Settings;
import gridool.util.primitive.Primitives;

import java.io.File;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class JettyService implements GridService {
    private static final Log LOG = LogFactory.getLog(JettyService.class);

    @Nonnull
    private final String resourceDir;

    @Nullable
    private Server server;

    public JettyService(String resourceDir) {
        if(resourceDir == null) {
            throw new IllegalArgumentException();
        }
        this.resourceDir = resourceDir;
    }

    @Override
    public String getServiceName() {
        return JettyService.class.getName();
    }

    @Override
    public boolean isDaemon() {
        return true;
    }

    @Override
    public void start() throws GridException {
        File file = new File(resourceDir);
        if(!file.exists()) {
            throw new GridException("gridool.http.jetty.rdir=" + resourceDir + " is not found");
        }

        final Server server = new Server();

        int port = Primitives.parseInt(Settings.getThroughSystemProperty("gridool.http.jetty.port"), 8081);
        SelectChannelConnector connector = new SelectChannelConnector();
        connector.setPort(port);
        server.addConnector(connector);

        ResourceHandler handler = new ResourceHandler();
        handler.setResourceBase(file.getAbsolutePath());
        server.setHandler(handler);

        try {
            server.start();
        } catch (Exception e) {
            throw new GridException(e);
        }

        LOG.info("Jetty started at " + connector.getHost() + ':' + port);
        this.server = server;
    }

    @Override
    public void stop() throws GridException {
        if(server != null) {
            try {
                server.stop();
            } catch (Exception e) {
                LOG.debug("failed to stop Jetty", e);
            }
        }
    }

}
