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
package gridool.dfs;

import gridool.GridConfiguration;
import gridool.GridException;
import gridool.GridNode;
import gridool.GridResourceRegistry;
import gridool.GridService;
import gridool.Settings;
import gridool.util.GridUtils;
import gridool.util.concurrent.ExecutorFactory;
import gridool.util.primitive.Primitives;
import gridool.util.xfer.RecievedFileWriter;
import gridool.util.xfer.TransferRequestListener;
import gridool.util.xfer.TransferServer;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridDfsService implements GridService {
    public static final int DEFAULT_RECV_PORT = 47110;

    private final GridConfiguration config;

    private final GridDfsClient client;
    private final ExecutorService recvExecs;

    public GridDfsService(@Nonnull final GridConfiguration config, @Nonnull GridResourceRegistry resourceRegistry) {
        this.config = config;
        this.client = new GridDfsClient();
        this.recvExecs = ExecutorFactory.newSingleThreadExecutor("FileReceiver", true);
        resourceRegistry.setDfsService(this);
    }

    public String getServiceName() {
        return GridDfsService.class.getName();
    }

    public boolean isDaemon() {
        return true;
    }

    public GridDfsClient getDFSClient() {
        return client;
    }

    public void start() throws GridException {
        // run file receiver
        int recvFileConcurrency = Primitives.parseInt(Settings.get("gridool.dfs.file_receiver.concurrency"), 2);
        int port = config.getFileReceiverPort();
        final TransferServer xferServer = createTransferServer(client, recvFileConcurrency, port);
        try {
            xferServer.setup(port);
        } catch (IOException e) {
            throw new GridException("failed to setup TransferServer", e);
        }
        recvExecs.submit(xferServer);
    }

    public void stop() throws GridException {
        recvExecs.shutdownNow();
    }

    private static TransferServer createTransferServer(GridDfsClient client, @Nonnegative int concurrency, int fileRecvPort) {
        File colDir = GridUtils.getWorkDir(true);
        TransferRequestListener listener = new RecievedFileWriter(colDir); //new ReplicatableRecvFileWriter(client, colDir, fileRecvPort);
        int priority = Primitives.parseInt(Settings.get("gridool.dfs.file_receiver.thread_priority"), Thread.NORM_PRIORITY);
        return new TransferServer(concurrency, priority, listener);
    }

    @Deprecated
    private static final class ReplicatableRecvFileWriter extends RecievedFileWriter {

        private final GridDfsClient clinet;
        private final GridNode localNode;
        private final int fileRecvPort;

        private boolean replicate = false;
        private String dirPath;
        private boolean append;
        private boolean sync;

        public ReplicatableRecvFileWriter(GridDfsClient clinet, File baseDir, GridNode localNode, int fileRecvPort) {
            super(baseDir);
            this.clinet = clinet;
            this.localNode = localNode;
            this.fileRecvPort = fileRecvPort;
        }

        @Override
        protected void readAdditionalHeader(DataInputStream in, String fname, String dirPath, long len, boolean append, boolean sync)
                throws IOException {
            this.replicate = in.readBoolean();
            this.dirPath = dirPath;
            this.append = append;
            this.sync = sync;
        }

        @Override
        protected void postFileAppend(File file, long startPos, long len) throws IOException {
            if(replicate) {
                List<GridNode> replicas = localNode.getReplicas();
                for(GridNode node : replicas) {
                    InetAddress dstAddr = node.getPhysicalAdress();
                    clinet.sendfile(file, startPos, len, dirPath, dstAddr, fileRecvPort, append, sync);
                }
            }
        }
    }

}
