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
package monetdb;

import gridool.db.helpers.MonetDBUtils;

import java.io.IOException;

import nl.cwi.monetdb.merovingian.Control;
import nl.cwi.monetdb.merovingian.MerovingianException;
import nl.cwi.monetdb.merovingian.SabaothDB;
import nl.cwi.monetdb.merovingian.SabaothDB.SABdbState;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class RestartMonetDB {

    public RestartMonetDB() {}

    public static void main(String[] args) throws MerovingianException, IOException {
        String connectUrl = "jdbc:monetdb://localhost/tpch001";
        String dbname = MonetDBUtils.getDbName(connectUrl);

        Control ctrl = new Control("localhost", 50001, "qz4wrc8JEHjt6z02DQ1Lfd3JJ");
        SabaothDB db = ctrl.getStatus(dbname);
        SabaothDB.SABdbState state = db.getState();
        if(state == SABdbState.SABdbRunning) {
            ctrl.stop(dbname);
        } else {
            System.err.println(state);
        }
        ctrl.start(dbname);
    }

}
