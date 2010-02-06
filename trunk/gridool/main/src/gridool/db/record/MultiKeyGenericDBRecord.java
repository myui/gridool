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
public final class MultiKeyGenericDBRecord extends GenericDBRecord {

    private transient int[] pkeyIndexes;
    private transient int[] fkeyIndexes;

    private transient byte[][] pfKeys;

    public MultiKeyGenericDBRecord() {}

    public void configureRecord(@Nullable int[] pfKeyIndexes, @Nullable int[] fkeyIndexes) {
        this.pkeyIndexes = pfKeyIndexes;
        this.fkeyIndexes = fkeyIndexes;
        int len = (pfKeyIndexes == null ? 0 : 1) + (fkeyIndexes == null ? 0 : fkeyIndexes.length);
        this.pfKeys = new byte[len][];
    }

    public byte[][] getKeys() {
        return pfKeys;
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        final ResultSetMetaData meta = resultSet.getMetaData();
        final int cols = meta.getColumnCount();
        if(cols > 0) {
            final Object[] columns = new Object[cols];
            int[] types = null;
            for(int i = 0; i < cols; i++) {
                Object col = resultSet.getObject(i + 1);
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
    }

}
