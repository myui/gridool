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

import gridool.db.helpers.ForeignKey;
import gridool.db.helpers.PrimaryKey;
import gridool.db.partitioning.DBPartitioningJobConf;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;

import javax.annotation.Nonnull;

import xbird.util.io.IOUtils;
import xbird.util.struct.Pair;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class PartitioningJobConf implements Externalizable {
    private static final long serialVersionUID = -7526401021725650012L;

    private/* final */String[] lines;
    private/* final */String fileName;
    private/* final */boolean isFirst;
    private/* final */Pair<PrimaryKey, Collection<ForeignKey>> primaryForeignKeys;
    private/* final */DBPartitioningJobConf jobConf;

    public PartitioningJobConf() {}

    public PartitioningJobConf(@Nonnull String[] lines, @Nonnull String fileName, boolean isFirst, @Nonnull Pair<PrimaryKey, Collection<ForeignKey>> primaryForeignKeys, @Nonnull DBPartitioningJobConf jobConf) {
        this.lines = lines;
        this.fileName = fileName;
        this.isFirst = isFirst;
        this.primaryForeignKeys = primaryForeignKeys;
        this.jobConf = jobConf;
    }

    public String[] getLines() {
        return lines;
    }

    public String getFileName() {
        return fileName;
    }

    public boolean isFirst() {
        return isFirst;
    }

    public Pair<PrimaryKey, Collection<ForeignKey>> getPrimaryForeignKeys() {
        return primaryForeignKeys;
    }

    public DBPartitioningJobConf getJobConf() {
        return jobConf;
    }

    @SuppressWarnings("unchecked")
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int numLines = in.readInt();
        final String[] lines = new String[numLines];
        for(int i = 0; i < numLines; i++) {
            lines[i] = IOUtils.readString(in);
        }
        this.lines = lines;
        this.fileName = IOUtils.readString(in);
        this.isFirst = in.readBoolean();
        this.primaryForeignKeys = (Pair<PrimaryKey, Collection<ForeignKey>>) in.readObject();
        this.jobConf = (DBPartitioningJobConf) in.readObject();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(lines.length);
        for(final String line : lines) {
            IOUtils.writeString(line, out);
        }
        IOUtils.writeString(fileName, out);
        out.writeBoolean(isFirst);
        out.writeObject(primaryForeignKeys);
        out.writeObject(jobConf);
    }

}