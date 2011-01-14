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
package gridool.util.jdbc.handlers;

import gridool.util.jdbc.ResultSetHandler;

import java.sql.ResultSet;
import java.sql.SQLException;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class ScalarHandler implements ResultSetHandler {

    private final int columnIndex;
    private final String columnName;

    public ScalarHandler(String columnName) {
        this.columnIndex = -1;
        this.columnName = columnName;
    }

    public ScalarHandler(int columnIndex) {
        this.columnIndex = columnIndex;
        this.columnName = null;
    }

    public Object handle(final ResultSet rs) throws SQLException {
        if(rs.next()) {
            if(columnName == null) {
                return rs.getObject(columnIndex);
            } else {
                return rs.getObject(columnName);
            }
        } else {
            return null;
        }
    }

}
