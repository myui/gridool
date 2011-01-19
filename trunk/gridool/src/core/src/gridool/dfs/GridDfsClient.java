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

import gridool.Settings;
import gridool.util.concurrent.ExecutorFactory;
import gridool.util.io.FastByteArrayOutputStream;
import gridool.util.primitive.Primitives;
import gridool.util.xfer.RunnableFileSender;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridDfsClient {

    public static final int SENDER_CONCURRENCY;
    static {
        SENDER_CONCURRENCY = Primitives.parseInt(Settings.get("gridool.dfs.file_sender.concurrency"), 3);
    }

    private final ExecutorService sencExecs;

    public GridDfsClient() {
        this.sencExecs = ExecutorFactory.newFixedThreadPool(SENDER_CONCURRENCY, "FileSender", true);
    }

    public Future<?> sendfile(@Nonnull final File file, @Nullable final String writeDirPath, @Nonnull final InetAddress dstAddr, final int dstPort, final boolean append, final boolean sync)
            throws IOException {
        RunnableFileSender run = new RunnableFileSender(file, writeDirPath, dstAddr, dstPort, append, sync);
        return sencExecs.submit(run);
    }

    public Future<?> sendfile(@Nonnull final File file, long pos, long count, @Nullable final String writeDirPath, @Nonnull final InetAddress dstAddr, final int dstPort, final boolean append, final boolean sync)
            throws IOException {
        RunnableFileSender run = new RunnableFileSender(file, pos, count, writeDirPath, dstAddr, dstPort, append, sync);
        return sencExecs.submit(run);
    }

    public Future<?> send(@Nonnull final FastByteArrayOutputStream data, @Nonnull final String fileName, @Nullable final String writeDirPath, @Nonnull final InetAddress dstAddr, final int dstPort, final boolean append, final boolean sync) {
        RunnableFileSender run = new RunnableFileSender(data, fileName, writeDirPath, dstAddr, dstPort, append, sync);
        return sencExecs.submit(run);
    }

}
