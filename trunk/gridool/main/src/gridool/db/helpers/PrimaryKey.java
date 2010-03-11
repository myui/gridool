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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import xbird.util.collections.ints.IntArrayList;
import xbird.util.io.IOUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class PrimaryKey implements ConstraintKey, Externalizable {

    private/* final */String pkName;
    private/* final */String tableName;
    private/* final */List<String> columnNames;

    // additional entries
    @Nullable
    private/* final */IntArrayList columnPositions;
    @Nonnull
    private List<ForeignKey> exportedKeys = Collections.emptyList();

    public PrimaryKey() {} // for Externalizable

    public PrimaryKey(@Nonnull String pkName, @Nonnull String tableName) {
        super();
        this.pkName = pkName;
        this.tableName = tableName;
        this.columnNames = new ArrayList<String>(2);
    }

    public void addColumn(@Nonnull final ResultSet rs) throws SQLException {
        final String columnName = rs.getString("COLUMN_NAME");
        if(columnName == null) {
            throw new IllegalStateException();
        }
        columnNames.add(columnName);
    }

    public void setColumnPositions(@CheckForNull final DatabaseMetaData metadata)
            throws SQLException {
        if(metadata == null) {
            throw new IllegalArgumentException();
        }
        final int numColumns = columnNames.size();
        final IntArrayList positions = new IntArrayList(numColumns);
        for(String columnName : columnNames) {
            final ResultSet colrs = metadata.getColumns(null, null, tableName, columnName);
            try {
                if(!colrs.next()) {
                    throw new IllegalStateException("Existing PK column not defined: " + tableName
                            + '.' + columnName);
                }
                int pos = colrs.getInt("ORDINAL_POSITION");
                positions.add(pos);
                assert (!colrs.next());
            } finally {
                colrs.close();
            }
        }
        this.columnPositions = positions;
    }

    public boolean isPrimaryKey() {
        return true;
    }

    @Nonnull
    public String getConstraintName() {
        return getPkName();
    }

    @Nonnull
    public String getTableName() {
        return tableName;
    }

    @Nonnull
    public String getPkName() {
        return pkName;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public int[] getColumnPositions(boolean sort) {
        if(columnPositions == null) {
            throw new UnsupportedOperationException("columnPositions is not reserved");
        }
        final int[] pos = columnPositions.toArray();
        if(sort) {
            Arrays.sort(pos);
        }
        return pos;
    }

    @Nullable
    public List<ForeignKey> getExportedKeys() {
        return exportedKeys;
    }

    public void setExportedKeys(List<ForeignKey> exportedKeyList) {
        this.exportedKeys = exportedKeyList;
    }

    public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
        this.pkName = IOUtils.readString(in);
        this.tableName = IOUtils.readString(in);
        final int numNames = in.readInt();
        final List<String> names = new ArrayList<String>(numNames);
        for(int i = 0; i < numNames; i++) {
            String name = IOUtils.readString(in);
            names.add(name);
        }
        this.columnNames = names;
        final int numPos = in.readShort();
        if(numPos == -1) {
            this.columnPositions = null;
        } else {
            final IntArrayList positions = new IntArrayList(numPos);
            for(int i = 0; i < numPos; i++) {
                int pos = in.readInt();
                positions.add(pos);
            }
            this.columnPositions = positions;
        }
        final int numExportedKeys = in.readInt();
        this.exportedKeys = new ArrayList<ForeignKey>(numExportedKeys);
        for(int i = 0; i < numExportedKeys; i++) {
            ForeignKey fk = (ForeignKey) in.readObject();
            exportedKeys.add(fk);
        }
    }

    public void writeExternal(final ObjectOutput out) throws IOException {
        IOUtils.writeString(pkName, out);
        IOUtils.writeString(tableName, out);
        final int numNames = columnNames.size();
        out.writeInt(numNames);
        for(int i = 0; i < numNames; i++) {
            String name = columnNames.get(i);
            IOUtils.writeString(name, out);
        }
        if(columnPositions == null) {
            out.writeShort(-1);
        } else {
            final int numPos = columnPositions.size();
            out.writeShort(numPos);
            for(int i = 0; i < numPos; i++) {
                int pos = columnPositions.get(i);
                out.writeInt(pos);
            }
        }
        final int numExportedKeys = exportedKeys.size();
        out.writeInt(numExportedKeys);
        for(int i = 0; i < numExportedKeys; i++) {
            ForeignKey fk = exportedKeys.get(i);
            out.writeObject(fk);
        }
    }

}
