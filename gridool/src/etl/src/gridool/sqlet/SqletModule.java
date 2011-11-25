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
package gridool.sqlet;

import gridool.sqlet.api.SqletCommand;
import gridool.sqlet.catalog.MapReduceConf;
import gridool.sqlet.catalog.PartitioningConf;
import gridool.sqlet.catalog.SystemCatalog;

import java.util.LinkedList;
import java.util.Queue;

import javax.annotation.Nonnull;

/**
 * @author Makoto YUI
 */
public final class SqletModule {

    private final SystemCatalog catalog;
    private final Queue<SqletCommand> cmdQueue;

    public SqletModule(SystemCatalog catalog) {
        this.catalog = catalog;
        this.cmdQueue = new LinkedList<SqletCommand>();
    }

    public PartitioningConf getPartitioningConf(@Nonnull String catalogName) {
        return catalog.getPartitioningConf(catalogName);
    }

    public MapReduceConf getMapReduceConf(@Nonnull String catalogName) {
        return catalog.getMapReduceConf(catalogName);
    }
    
    public void offerCommand(SqletCommand cmd) {
        cmdQueue.offer(cmd);
    }

    @Override
    public String toString() {
        return "SqletModule [catalog=" + catalog + ", cmdQueue=" + cmdQueue + "]";
    }
}
