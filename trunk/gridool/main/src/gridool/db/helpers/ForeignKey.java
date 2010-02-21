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
package gridool.db.helpers;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import xbird.util.collections.ints.IntArrayList;
import xbird.util.io.IOUtils;

import com.sun.istack.internal.Nullable;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class ForeignKey implements ConstraintKey, Externalizable {

    private/* final */String fkName; // useful for debugging
    private/* final */String fkTableName;
    private/* final */List<String> fkColumnNames;
    @Nullable
    private/* final */IntArrayList fkColumnPositions;

    // references
    private/* final */String pkTableName;
    private/* final */List<String> pkColumnNames;

    public ForeignKey() {}// for Externalizable

    public ForeignKey(@CheckForNull String fkName, @CheckForNull String fkTableName, @CheckForNull String pkTableName, boolean reserveFkColumnPosition) {
        if(fkName == null) {
            throw new IllegalArgumentException();
        }
        if(fkTableName == null) {
            throw new IllegalArgumentException();
        }
        if(pkTableName == null) {
            throw new IllegalArgumentException();
        }
        this.fkName = fkName;
        this.fkTableName = fkTableName;
        this.fkColumnNames = new ArrayList<String>(2);
        this.pkTableName = pkTableName;
        this.pkColumnNames = new ArrayList<String>(2);
        this.fkColumnPositions = reserveFkColumnPosition ? new IntArrayList(2) : null;
    }

    public void addColumn(@Nonnull final ResultSet rs, @CheckForNull final DatabaseMetaData metadata)
            throws SQLException {
        final String fkColumn = rs.getString("FKCOLUMN_NAME");
        if(fkColumn == null) {
            throw new IllegalStateException();
        }
        fkColumnNames.add(fkColumn);
        if(fkColumnPositions != null) {
            if(metadata == null) {
                throw new IllegalArgumentException();
            }
            final ResultSet colrs = metadata.getColumns(null, null, fkTableName, fkColumn);
            try {
                if(!colrs.next()) {
                    throw new IllegalStateException("Existing FK column not defined: "
                            + fkTableName + '.' + fkColumn);
                }
                int pos = colrs.getInt("ORDINAL_POSITION");
                fkColumnPositions.add(pos);
                assert (!colrs.next());
            } finally {
                colrs.close();
            }
        }
        final String pkColumn = rs.getString("PKCOLUMN_NAME");
        if(pkColumn == null) {
            throw new IllegalStateException();
        }
        pkColumnNames.add(pkColumn);
    }

    @Nonnull
    public String getConstraintName() {
        return getFkName();
    }

    @Nonnull
    public String getTableName() {
        return getFkTableName();
    }

    @Nonnull
    public String getFkName() {
        return fkName;
    }

    @Nonnull
    public String getFkTableName() {
        return fkTableName;
    }

    @Nonnull
    public List<String> getFkColumnNames() {
        return fkColumnNames;
    }

    @Nonnull
    public int[] getForeignColumnPositions() {
        if(fkColumnPositions == null) {
            throw new UnsupportedOperationException("ForeignColumnPositions is not reserved");
        }
        return fkColumnPositions.toArray();
    }

    @Nonnull
    public String getPkTableName() {
        return pkTableName;
    }

    @Nonnull
    public List<String> getPkColumnNames() {
        return pkColumnNames;
    }

    @Override
    public int hashCode() {
        return fkName.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        }
        if(obj instanceof ForeignKey) {
            ForeignKey other = (ForeignKey) obj;
            return fkName.equals(other.fkName);
        }
        return false;
    }

    @Override
    public String toString() {
        return fkName;
    }

    public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
        this.fkName = IOUtils.readString(in);
        this.fkTableName = IOUtils.readString(in);
        final int numFkColumns = in.readShort();
        final List<String> fkColumns = new ArrayList<String>(numFkColumns);
        for(int i = 0; i < numFkColumns; i++) {
            String e = IOUtils.readString(in);
            fkColumns.add(e);
        }
        this.fkColumnNames = fkColumns;
        final int numPositions = in.readShort();
        if(numPositions == -1) {
            this.fkColumnPositions = null;
        } else {
            IntArrayList positions = new IntArrayList(numPositions);
            for(int i = 0; i < numPositions; i++) {
                int pos = in.readInt();
                positions.add(pos);
            }
            this.fkColumnPositions = positions;
        }
        this.pkTableName = IOUtils.readString(in);
        final int numPkColumns = in.readInt();
        final List<String> pkColumns = new ArrayList<String>(numPkColumns);
        for(int i = 0; i < numPkColumns; i++) {
            String e = IOUtils.readString(in);
            pkColumns.add(e);
        }
        this.pkColumnNames = pkColumns;
    }

    public void writeExternal(final ObjectOutput out) throws IOException {
        IOUtils.writeString(fkName, out);
        IOUtils.writeString(fkTableName, out);
        final int numFkColumns = fkColumnNames.size();
        out.writeShort(numFkColumns);
        for(int i = 0; i < numFkColumns; i++) {
            String s = fkColumnNames.get(i);
            IOUtils.writeString(s, out);
        }
        if(fkColumnPositions == null) {
            out.writeShort(-1);
        } else {
            final int numPositions = fkColumnPositions.size();
            out.writeShort(numPositions);
            for(int i = 0; i < numPositions; i++) {
                int pos = fkColumnPositions.get(i);
                out.writeInt(pos);
            }
        }
        IOUtils.writeString(pkTableName, out);
        final int numPkColumns = pkColumnNames.size();
        out.writeInt(numPkColumns);
        for(int i = 0; i < numPkColumns; i++) {
            String s = pkColumnNames.get(i);
            IOUtils.writeString(s, out);
        }
    }

}