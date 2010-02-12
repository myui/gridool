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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import javax.annotation.Nullable;

import xbird.util.lang.ArrayUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class MultiKeyRowPlaceholderRecord implements DBRecord, Externalizable {

    private transient int[] pkeyIndexes;
    private transient int[] fkeyIndexes;
    private transient byte[][] pfKeys;

    private String row;

    public MultiKeyRowPlaceholderRecord() {}//for Externalizable

    public void configureRecord(@Nullable int[] pfKeyIndexes, @Nullable int[] fkeyIndexes) {
        this.pkeyIndexes = pfKeyIndexes;
        this.fkeyIndexes = fkeyIndexes;
        int len = (pfKeyIndexes == null ? 0 : 1) + (fkeyIndexes == null ? 0 : fkeyIndexes.length);
        this.pfKeys = new byte[len][];
    }

    public byte[][] getKeys() {
        return pfKeys;
    }

    public String getRow() {
        return row;
    }

    protected int expectedLineSize() {
        return 64;
    }

    public String getFieldSeparator() {
        return "\t";
    }

    public String getRecordSeparator() {
        return "\n";
    }

    public String getStringQuote() {
        return "\"";
    }

    public String getNullString() {
        return "";
    }

    protected String escapeString(String s) {
        return s;
    }

    public void readFields(ResultSet resultSet) throws SQLException {
        final ResultSetMetaData meta = resultSet.getMetaData();
        final int cols = meta.getColumnCount();
        if(cols <= 0) {
            throw new IllegalStateException("No column in the ResultSet");
        }
        final String quote = getStringQuote();
        final String fieldSep = getFieldSeparator();
        final String recSep = getRecordSeparator();
        final String nullStr = getNullString();
        final StringBuilder line = new StringBuilder(expectedLineSize());
        for(int i = 1; i <= cols; i++) {
            if(i != 1) {
                line.append(fieldSep);
            }
            final String column = resultSet.getString(i);
            if(column == null) {
                line.append(nullStr);
            } else {
                final String columnClass = meta.getColumnClassName(i);
                if("java.lang.String".equals(columnClass)) {
                    line.append(quote);
                    line.append(escapeString(column));
                    line.append(quote);
                } else {
                    line.append(column);
                }
            }
        }
        line.append(recSep);
        this.row = line.toString();

        int idx = 0;
        if(pkeyIndexes != null) {
            byte[] pkey = new byte[0];
            for(int i : pkeyIndexes) {
                byte[] b = resultSet.getBytes(i);
                pkey = ArrayUtils.append(pkey, b);
            }
            pfKeys[idx++] = pkey;
        }
        if(fkeyIndexes != null) {
            for(int i : fkeyIndexes) {
                byte[] b = resultSet.getBytes(i);
                pfKeys[idx++] = b;
            }
        }
    }

    public byte[] getKey() {
        throw new UnsupportedOperationException();
    }

    public int getNumFields() {
        throw new UnsupportedOperationException();
    }

    public void writeFields(PreparedStatement statement) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    public void writeTo(GridMarshaller marshaller, OutputStream out) throws GridException {
        throw new UnsupportedOperationException();
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        throw new UnsupportedOperationException();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

}
