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
package gridool.db.partitioning.csv;

import gridool.GridException;
import gridool.GridJob;
import gridool.GridNode;
import gridool.construct.GridTaskAdapter;
import gridool.util.GridUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import xbird.util.io.IOUtils;
import xbird.util.nio.NIOUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 * @deprecated
 */
public final class FileAppendTask extends GridTaskAdapter {
    private static final long serialVersionUID = 4313317121395228308L;

    @Nonnull
    private transient/* final */String fileName;
    @Nonnull
    private transient/* final */byte[] rowsData;
    private transient/* final */boolean append;
    private transient/* final */boolean replicate;

    @Nullable
    private transient GridNode masterNode;

    public FileAppendTask(GridJob<?, ?> job, @Nonnull String fileName, @Nonnull byte[] rowsData, boolean append, boolean replicate) {
        super(job, false);
        this.fileName = fileName;
        this.rowsData = rowsData;
        this.append = append;
        this.replicate = replicate;
    }

    @Override
    public boolean isReplicatable() {
        return replicate;
    }

    @Override
    public void setTransferToReplica(@Nonnull GridNode masterNode) {
        this.masterNode = masterNode;
    }

    protected Serializable execute() throws GridException {
        appendToFile(fileName, rowsData, append);
        return null;
    }

    private static File appendToFile(final String fileName, final byte[] data, final boolean append)
            throws GridException {
        File colDir = GridUtils.getWorkDir(true);
        final File file = new File(colDir, fileName);

        final ByteBuffer buf = ByteBuffer.wrap(data);
        FileOutputStream fos = null;
        FileChannel ch = null;
        try {
            fos = new FileOutputStream(file, append); // note that FileOutputStream takes exclusive lock            
            ch = fos.getChannel();
            NIOUtils.writeFully(ch, buf);
            //fos.write(data, 0, data.length); // REVIEWME 
            //fos.flush();
        } catch (IOException ioe) {
            throw new GridException("Failed to write data into file: " + file.getAbsolutePath(), ioe);
        } finally {
            IOUtils.closeQuietly(ch, fos);
        }
        return file;
    }

    private void writeObject(ObjectOutputStream s) throws java.io.IOException {
        s.defaultWriteObject();
        if(masterNode != null) {
            String altered = GridUtils.alterFileName(fileName, masterNode);
            IOUtils.writeString(altered, s);
        } else {
            IOUtils.writeString(fileName, s);
        }
        IOUtils.writeBytes(rowsData, s);
        s.writeBoolean(append);
        s.writeBoolean(replicate);
    }

    private void readObject(ObjectInputStream s) throws java.io.IOException, ClassNotFoundException {
        s.defaultReadObject();
        this.fileName = IOUtils.readString(s);
        this.rowsData = IOUtils.readBytes(s);
        this.append = s.readBoolean();
        this.replicate = s.readBoolean();
    }
}