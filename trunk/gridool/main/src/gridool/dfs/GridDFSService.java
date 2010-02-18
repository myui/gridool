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
import gridool.GridService;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import xbird.config.Settings;
import xbird.storage.DbCollection;
import xbird.util.concurrent.ExecutorFactory;
import xbird.util.xfer.RecievedFileWriter;
import xbird.util.xfer.TransferRequestListener;
import xbird.util.xfer.TransferServer;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class GridDFSService implements GridService {

    private final GridConfiguration config;
    private final ExecutorService recvExecs;

    public GridDFSService(@Nonnull final GridConfiguration config) {
        this.config = config;
        this.recvExecs = ExecutorFactory.newSingleThreadExecutor("FileReceiver", true);
    }

    public String getServiceName() {
        return GridDFSService.class.getName();
    }

    public boolean isDaemon() {
        return true;
    }

    public void start() throws GridException {
        // run file receiver
        int recvFileConcurrency = Integer.parseInt(Settings.get("gridool.dfs.file_receiver.concurrency", "2"));
        int port = config.getFileReceiverPort();
        final TransferServer xferServer = createTransferServer(recvFileConcurrency);
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

    private static TransferServer createTransferServer(@Nonnegative int concurrency) {
        DbCollection rootCol = DbCollection.getRootCollection();
        File colDir = rootCol.getDirectory();
        TransferRequestListener listener = new RecievedFileWriter(colDir, true);
        return new TransferServer(concurrency, listener);
    }

}
