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
package gridool.db.partitioning;

import gridool.GridJob;
import gridool.GridNode;
import gridool.db.partitioning.csv.PartitioningJobConf;
import gridool.db.partitioning.csv.dist.GlobalCsvHashPartitioningJob;
import gridool.db.partitioning.csv.dist.LocalCsvHashPartitioningJob;
import gridool.db.partitioning.csv.distmm.InMemoryLocalCsvHashPartitioningJob;
import gridool.db.partitioning.csv.grace.CsvGraceHashPartitioningJob;
import gridool.db.partitioning.csv.normal.CsvHashPartitioningJob;
import gridool.db.partitioning.csv.pgrace.ParallelGraceLocalCsvHashPartitioningJob;
import gridool.routing.GridRouter;
import gridool.util.primitive.MutableInt;
import gridool.util.struct.Pair;

import java.util.HashMap;
import java.util.Map;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public enum PartitioningJobType {

    normal, distributed, distributedInMemoryIndex, localGrace, parallelGrace, multiInputsParallelGrace;

    private PartitioningJobType() {}

    public boolean isGrace() {
        return (this == localGrace) || (this == parallelGrace)
                || (this == multiInputsParallelGrace);
    }

    public Class<? extends GridJob<PartitioningJobConf, Map<GridNode, MutableInt>>> getFirstPartitioningJobClass() {
        switch(this) {
            case normal:
                return CsvHashPartitioningJob.class;
            case distributed:
            case distributedInMemoryIndex:
            case parallelGrace:
            case multiInputsParallelGrace:
                return GlobalCsvHashPartitioningJob.class;
            case localGrace:
                return CsvGraceHashPartitioningJob.class;
            default:
                throw new IllegalStateException("Unexpected PartitioningJobType: "
                        + this.toString());
        }
    }

    public Class<? extends GridJob<Pair<PartitioningJobConf, GridRouter>, HashMap<GridNode, MutableInt>>> getLocalPartitioningJobClass() {
        switch(this) {
            case distributed:
                return LocalCsvHashPartitioningJob.class;
            case distributedInMemoryIndex:
                return InMemoryLocalCsvHashPartitioningJob.class;
            case parallelGrace:
            case multiInputsParallelGrace:
                return ParallelGraceLocalCsvHashPartitioningJob.class;
            default:
                throw new IllegalStateException("Unexpected PartitioningJobType: "
                        + this.toString());
        }
    }

    public static PartitioningJobType resolve(String typeName) {
        if("normal".equalsIgnoreCase(typeName)) {
            return normal;
        } else if("distributed".equalsIgnoreCase(typeName) || "dist".equalsIgnoreCase(typeName)) {
            return distributed;
        } else if("distributedInMemoryIndex".equalsIgnoreCase(typeName)
                || "distmm".equalsIgnoreCase(typeName)) {
            return distributedInMemoryIndex;
        } else if("localGrace".equalsIgnoreCase(typeName) || "grace".equalsIgnoreCase(typeName)) {
            return localGrace;
        } else if("parallelGrace".equalsIgnoreCase(typeName) || "pgrace".equalsIgnoreCase(typeName)) {
            return parallelGrace;
        } else if("multiInputsParallelGrace".equalsIgnoreCase(typeName)
                || "mpgrace".equals(typeName)) {
            return multiInputsParallelGrace;
        }
        throw new IllegalArgumentException("Illegal PartitioningJobType name: " + typeName);
    }

}
