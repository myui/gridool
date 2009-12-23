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

import java.io.Serializable;

import javax.annotation.Nonnull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public abstract class PartitioningJobConf implements Serializable {
    private static final long serialVersionUID = 636573640789390674L;

    public PartitioningJobConf() {}

    @Nonnull
    public abstract String getCsvFilePath();

    @Nonnull
    public abstract String getTableName();

    @Nonnull
    public abstract int[] partitionigKeyIndices();

    public char getFieldSeparator() {
        return '\t';
    }

    public char getRecordSeparator() {
        return '\n';
    }

    public char getStringQuote() {
        return '\"';
    }

}
