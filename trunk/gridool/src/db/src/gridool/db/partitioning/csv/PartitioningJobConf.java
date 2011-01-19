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
import gridool.util.io.IOUtils;
import gridool.util.struct.Pair;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class PartitioningJobConf implements Externalizable {
    private static final long serialVersionUID = -7526401021725650012L;

    private/* final */String[] lines;
    private/* final */String fileName;
    private/* final */boolean isFirst;
    private/* final */Pair<PrimaryKey, Collection<ForeignKey>> primaryForeignKeys;
    private/* final */DBPartitioningJobConf jobConf;
    private/* final */int bucketNumber;
    
    @Nullable
    private transient Map<String, OutputStream> outputMap = null;

    public PartitioningJobConf() {}

    public PartitioningJobConf(@CheckForNull String[] lines, @CheckForNull String fileName, boolean isFirst, @CheckForNull Pair<PrimaryKey, Collection<ForeignKey>> primaryForeignKeys, @CheckForNull DBPartitioningJobConf jobConf) {
        this(lines, fileName, isFirst, primaryForeignKeys, jobConf, -1);
    }
    
    public PartitioningJobConf(@CheckForNull String[] lines, @CheckForNull String fileName, boolean isFirst, @CheckForNull Pair<PrimaryKey, Collection<ForeignKey>> primaryForeignKeys, @CheckForNull DBPartitioningJobConf jobConf, int bucketNumber) {
        if(lines == null) {
            throw new IllegalArgumentException("lines is not specfied");
        }
        if(fileName == null) {
            throw new IllegalArgumentException("fileName is not specfied");
        }
        if(primaryForeignKeys == null) {
            throw new IllegalArgumentException("primaryForeignKeys is not specfied");
        }
        if(jobConf == null) {
            throw new IllegalArgumentException("jobConf is not specfied");
        }
        this.lines = lines;
        this.fileName = fileName;
        this.isFirst = isFirst;
        this.primaryForeignKeys = primaryForeignKeys;
        this.jobConf = jobConf;
        this.bucketNumber = bucketNumber;
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

    public int getBucketNumber() {
        return bucketNumber;
    }

    public void setBucketNumber(int bucketNumber) {
        this.bucketNumber = bucketNumber;
    }

    public Map<String, OutputStream> getOutputMap() {
        return outputMap;
    }

    public void setOutputMap(Map<String, OutputStream> outputMap) {
        this.outputMap = outputMap;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int numLines = in.readInt();
        final String[] lines = new String[numLines];
        for(int i = 0; i < numLines; i++) {
            lines[i] = IOUtils.readString(in);
        }
        this.lines = lines;
        this.fileName = IOUtils.readString(in);
        this.isFirst = in.readBoolean();
        PrimaryKey pkey = (PrimaryKey) in.readObject();
        final int numFkeys = in.readInt();
        final List<ForeignKey> fkeys = new ArrayList<ForeignKey>(numFkeys);
        for(int i = 0; i < numFkeys; i++) {
            ForeignKey fk = (ForeignKey) in.readObject();
            fkeys.add(fk);
        }
        this.primaryForeignKeys = new Pair<PrimaryKey, Collection<ForeignKey>>(pkey, fkeys);
        this.jobConf = (DBPartitioningJobConf) in.readObject();
        this.bucketNumber = in.readInt();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(lines.length);
        for(final String line : lines) {
            IOUtils.writeString(line, out);
        }
        IOUtils.writeString(fileName, out);
        out.writeBoolean(isFirst);
        PrimaryKey pkey = primaryForeignKeys.getFirst();
        out.writeObject(pkey);
        Collection<ForeignKey> fkeys = primaryForeignKeys.getSecond();
        int numFkeys = fkeys.size();
        out.writeInt(numFkeys);
        for(final ForeignKey fk : fkeys) {
            out.writeObject(fk);
        }
        out.writeObject(jobConf);
        out.writeInt(bucketNumber);
    }

}