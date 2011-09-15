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
package gridool.discovery.file;

import gridool.GridConfiguration;
import gridool.GridException;
import gridool.GridNode;
import gridool.communication.payload.GridNodeInfo;
import gridool.discovery.DiscoveryServiceBase;
import gridool.util.io.IOUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class FileDiscoveryService extends DiscoveryServiceBase {
    private static final Log LOG = LogFactory.getLog(FileDiscoveryService.class);

    public FileDiscoveryService(GridConfiguration config) {
        super(config);
    }

    @Override
    public void start() throws GridException {
        GridNode localNode = config.getLocalNode();
        handleJoin(localNode);

        String userDir = System.getProperty("user.home");
        File file = new File(userDir, "servers.list");
        if(!file.exists()) {
            throw new GridException("Required file does not exist in: " + file.getAbsolutePath());
        }

        final FileReader fr;
        try {
            fr = new FileReader(file);
        } catch (FileNotFoundException e) {
            throw new GridException(e);
        }
        final BufferedReader br = new BufferedReader(fr, 8192);
        final StringBuilder buf = new StringBuilder(512);
        try {
            buf.append("{ ");
            int found = 0;
            String line;
            while(null != (line = br.readLine())) {
                String name = line.trim();
                String[] server = line.split(":");
                if(server.length == 2) {
                    String host = server[0];
                    int port = Integer.parseInt(server[1]);
                    InetAddress addr = InetAddress.getByName(host);
                    GridNode node = new GridNodeInfo(addr, port, false);
                    if(found != 0) {
                        buf.append(", ");
                    }
                    buf.append(name);
                    handleJoin(node);
                    found++;
                }
            }
            buf.append(" }\ntotal " + found + " nodes are joined to the cluster.");
        } catch (IOException e) {
            throw new GridException(e);
        } finally {
            IOUtils.closeQuietly(br);
        }
        if(LOG.isInfoEnabled()) {
            LOG.info(buf);
        }
    }

    @Override
    public void stop() throws GridException {}

}
