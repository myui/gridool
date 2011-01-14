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
package gridool.db.record;

import gridool.GridException;
import gridool.marshaller.GridMarshaller;
import gridool.util.string.StringUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class GenericDBRecord implements DBRecord, Externalizable {
    private static final long serialVersionUID = 8660692786480530500L;

    private transient byte[] key;
    protected Object[] results;
    @Nullable
    protected int[] columnTypes = null;

    public GenericDBRecord() {}

    /**
     * If there is a <code>null</code> column in the results, use {@link #GenericDBRecord(Object[], byte[], int[])} instead 
     * and specify the <code>columnTypes</code>.
     */
    public GenericDBRecord(@CheckForNull byte[] key, @CheckForNull Object... results) {
        if(key == null) {
            throw new IllegalArgumentException("No key was specidied");
        }
        if(results == null || results.length == 0) {
            throw new IllegalArgumentException("Illegal results");
        }
        this.key = key;
        this.results = results;
    }

    public GenericDBRecord(@CheckForNull byte[] key, @CheckForNull Object[] results, @Nonnull int[] columnTypes) {
        this(key, results);
        assert (columnTypes != null);
        this.columnTypes = columnTypes;
    }

    public byte[] getKey() {
        return key;
    }

    public int getNumFields() {
        return results == null ? -1 : results.length;
    }

    public void readFields(ResultSet resultSet) throws SQLException {
        final ResultSetMetaData meta = resultSet.getMetaData();
        final int cols = meta.getColumnCount();
        if(cols > 0) {
            final Object[] columns = new Object[cols];
            int[] types = null;
            for(int i = 0; i < cols; i++) {
                final Object col = resultSet.getObject(i + 1);
                if(i == 0) {
                    if(col instanceof String) {
                        this.key = StringUtils.getBytes((String) col);
                    } else {
                        this.key = resultSet.getBytes(1);
                    }
                }
                columns[i] = col;
                if(col == null) {
                    if(types == null) {
                        types = new int[cols];
                        this.columnTypes = types;
                    }
                    types[i] = meta.getColumnType(i + 1);
                }
            }
            this.results = columns;
        }
    }

    public void writeFields(PreparedStatement statement) throws SQLException {
        assert (results != null);
        final Object[] r = this.results;
        final int cols = r.length;
        for(int i = 0; i < cols; i++) {
            final Object col = r[i];
            if(col == null) {
                assert (columnTypes != null);
                statement.setNull(i + 1, columnTypes[i]);
            } else {
                statement.setObject(i + 1, col);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void writeTo(GridMarshaller marshaller, OutputStream out) throws GridException {
        for(Object obj : results) {
            marshaller.marshall(obj, out);
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int numResults = in.readInt();
        final Object[] objects = new Object[numResults];
        for(int i = 0; i < numResults; i++) {
            objects[i] = in.readObject();
        }
        this.results = objects;
        if(in.readBoolean()) {
            final int numTypes = in.readInt();
            final int[] types = new int[numTypes];
            for(int i = 0; i < numTypes; i++) {
                types[i] = in.readInt();
            }
            this.columnTypes = types;
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        final Object[] objects = results;
        final int numResults = objects.length;
        out.writeInt(numResults);
        for(int i = 0; i < numResults; i++) {
            out.writeObject(objects[i]);
        }
        final int[] types = columnTypes;
        if(types == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            final int numTypes = types.length;
            out.writeInt(numTypes);
            for(int i = 0; i < numResults; i++) {
                out.writeInt(types[i]);
            }
        }
    }

}
