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
package gridool.db.sql;

import gridool.db.sql.SQLTranslator.QueryString;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;
import junit.framework.TestCase;
import xbird.util.io.FileUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public class SQLTranslatorTest extends TestCase {

    /**
     * Test method for {@link gridool.db.sql.SQLTranslator#divideQuery(java.lang.String)}.
     * @throws IOException 
     */
    public void testDivideQuery() throws IOException {
        String query = FileUtils.toString(new File("/home/myui/workspace/gridool/examples/psql/tpch/15map.sql"));
        QueryString[] queries = SQLTranslator.divideQuery(query, true);
        for(QueryString q : queries) {
            System.out.print(q.getQuery());
            System.out.println(";\n");
        }
        Assert.assertEquals(3, queries.length);
    }

}
