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
package gridool.db.partitioning;

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
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.storage.DbCollection;
import xbird.util.io.FastBufferedOutputStream;
import xbird.util.io.IOUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class FileAppendTask extends GridTaskAdapter {
    private static final long serialVersionUID = 4313317121395228308L;
    private static final Log LOG = LogFactory.getLog(FileAppendTask.class);

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
        this.rowsData = null; // TODO REVIEWME memory leaking?
        return null;
    }

    private static File appendToFile(final String fileName, final byte[] data, final boolean append) {
        DbCollection rootColl = DbCollection.getRootCollection();
        final File colDir = rootColl.getDirectory();
        if(!colDir.exists()) {
            throw new IllegalStateException("Database directory not found: "
                    + colDir.getAbsoluteFile());
        }
        final File file;
        final FileOutputStream fos;
        try {
            file = new File(colDir, fileName);
            fos = new FileOutputStream(file, append); // note that FileOutputStream takes exclusive lock
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create a load file", e);
        }
        FileLock fileExLock = null;
        try {
            fileExLock = fos.getChannel().lock();
        } catch (OverlappingFileLockException oe) {
            if(LOG.isWarnEnabled()) {
                LOG.warn("failed to tryLock on a file: " + file.getAbsoluteFile(), oe);
            }
        } catch (IOException ioe) {
            LOG.error("failed to trylock on a file: " + file.getAbsolutePath(), ioe);
            throw new IllegalStateException(ioe);
        }
        try {
            FastBufferedOutputStream bos = new FastBufferedOutputStream(fos, 8192);
            bos.write(data, 0, data.length); // atomic writes in UNIX
            bos.flush();
            //bos.close();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write data into file: "
                    + file.getAbsolutePath(), e);
        } finally {
            if(fileExLock != null) {
                try {
                    fileExLock.release();
                } catch (IOException e) {
                    LOG.debug(e);
                }
            }
            try {
                fos.close();
            } catch (IOException ioe) {
                LOG.debug(ioe);
            }
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