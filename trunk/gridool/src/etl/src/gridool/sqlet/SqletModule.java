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
import javax.annotation.Nullable;

/**
 * @author Makoto YUI
 */
public final class SqletModule {

    private final SystemCatalog catalog;
    private final Queue<SqletCommand> cmdQueue;

    public SqletModule(@Nonnull SystemCatalog catalog) {
        this.catalog = catalog;
        this.cmdQueue = new LinkedList<SqletCommand>();
    }

    public PartitioningConf getPartitioningConf(@Nonnull String catalogName) {
        return catalog.getPartitioningConf(catalogName);
    }

    public MapReduceConf getMapReduceConf(@Nonnull String catalogName) {
        return catalog.getMapReduceConf(catalogName);
    }

    @Nonnull
    public PartitioningConf obtainPartitioningConf(@Nonnull String catalogName) {
        PartitioningConf conf = catalog.getPartitioningConf(catalogName);
        if(conf == null) {
            conf = new PartitioningConf();
            catalog.setPartitioningConf(catalogName, conf);
        }
        return conf;
    }

    @Nonnull
    public MapReduceConf obtainMapReduceConf(@Nonnull String catalogName) {
        MapReduceConf conf = catalog.getMapReduceConf(catalogName);
        if(conf == null) {
            conf = new MapReduceConf();
            catalog.setMapReduceConf(catalogName, conf);
        }
        return conf;
    }

    public void offerCommand(@Nonnull SqletCommand cmd) {
        cmdQueue.offer(cmd);
    }

    @Nullable
    public SqletCommand pollCommand() {
        return cmdQueue.poll();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("SqletModule [\n catalog=");
        builder.append(catalog);
        builder.append(",\n cmdQueue=");
        builder.append(cmdQueue);
        builder.append("\n]");
        return builder.toString();
    }

}
