/*
 * @(#)$Id$
 *
 * Copyright 2010-2011 Makoto YUI
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
package gridool.db.dba;

import java.io.File;


/**
 * @author Makoto YUI
 */
public final class MonetDBAccessor extends DBAccessor {

    public MonetDBAccessor() {
        super();
    }

    @Override
    public long copyToFile(String selectQuery, File outFile) {
        
        return 0;
    }

    @Override
    public long copyFromFile(String filepath, String tblName) {
        return 0;
    }


}
