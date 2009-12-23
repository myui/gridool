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
import gridool.construct.GridTaskAdapter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import javax.annotation.Nonnull;

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
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class FileAppendTask extends GridTaskAdapter {
    private static final long serialVersionUID = 4313317121395228308L;
    private static final Log LOG = LogFactory.getLog(FileAppendTask.class);

    @Nonnull
    private transient/* final */String fileName;
    @Nonnull
    private transient/* final */byte[] rowsData;

    @SuppressWarnings("unchecked")
    public FileAppendTask(GridJob job, @Nonnull String fileName, @Nonnull byte[] rowsData) {
        super(job, false);
    }

    public Serializable execute() throws GridException {
        appendToFile(fileName, rowsData);
        return null;
    }

    private static File appendToFile(final String fileName, final byte[] data) {
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
            fos = new FileOutputStream(file, true);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create a load file", e);
        }
        try {
            FastBufferedOutputStream bos = new FastBufferedOutputStream(fos, 8192);
            bos.write(data, 0, data.length);
            bos.flush();
            bos.close();
        } catch (IOException e) {
            try {
                fos.close();
            } catch (IOException ioe) {
                LOG.debug(ioe);
            }
            throw new IllegalStateException("Failed to write data into file: "
                    + file.getAbsolutePath(), e);
        }
        return file;
    }

    private void writeObject(ObjectOutputStream s) throws java.io.IOException {
        s.defaultWriteObject();
        IOUtils.writeString(fileName, s);
        IOUtils.writeBytes(rowsData, s);
    }

    private void readObject(ObjectInputStream s) throws java.io.IOException, ClassNotFoundException {
        s.defaultReadObject();
        this.fileName = IOUtils.readString(s);
        this.rowsData = IOUtils.readBytes(s);
    }
}