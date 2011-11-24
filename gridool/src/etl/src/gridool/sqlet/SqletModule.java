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

import gridool.sqlet.env.MapReduceConf;
import gridool.sqlet.env.PartitioningConf;
import gridool.sqlet.expr.SqletExpression;

/**
 * @author Makoto YUI
 */
public final class SqletModule implements SqletExpression {

    private final PartitioningConf partitioningConf;
    private final MapReduceConf mapredConf;
    
    public SqletModule() {
        this.partitioningConf = new PartitioningConf();
        this.mapredConf = new MapReduceConf();
    }

    public PartitioningConf getPartitioningConf() {
        return partitioningConf;
    }
    
    public MapReduceConf getMapReduceConf() {
        return mapredConf;
    }

}
