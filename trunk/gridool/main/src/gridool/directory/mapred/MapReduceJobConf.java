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
package gridool.directory.mapred;

import gridool.GridJob;
import gridool.directory.mapred.task.DhtMapShuffleTask;
import gridool.directory.mapred.task.DhtReduceTask;

import java.io.Serializable;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public abstract class MapReduceJobConf implements Serializable {
    private static final long serialVersionUID = -6387687169104905041L;

    public static final String OutputKeyCollectionName = "gridool#output-key-collections";

    @Nonnull
    protected String inputDhtName;

    public MapReduceJobConf(@CheckForNull String inputDhtName) {
        checkDhtName(inputDhtName);
        this.inputDhtName = inputDhtName;
    }

    private static void checkDhtName(final String dhtName) {
        if(dhtName == null) {
            throw new IllegalArgumentException();
        }
        if(dhtName.startsWith("gridool#")) {
            throw new IllegalArgumentException("DHT name starting with 'gridool#' is reserved: "
                    + dhtName);
        }
    }

    public String getInputDhtName() {
        return inputDhtName;
    }

    /**
     * Used by internal system only.
     */
    void setInputDhtName(@Nonnull String inputDhtName) {
        this.inputDhtName = inputDhtName;
    }

    /**
     * Shuffled keys are collected here.
     */
    public String getOutputKeyCollectionName() {
        return OutputKeyCollectionName;
    }

    protected int getNumMapTasks() {
        return Integer.MAX_VALUE;
    }

    protected int getNumReduceTasks() {
        return Integer.MAX_VALUE;
    }

    @SuppressWarnings("unchecked")
    @Nullable
    protected abstract DhtMapShuffleTask makeMapShuffleTask(@Nonnull GridJob job, @Nonnull String inputDhtName, @Nonnull String destDhtName);

    @SuppressWarnings("unchecked")
    @Nullable
    protected abstract DhtReduceTask makeReduceTask(@Nonnull GridJob job, @Nonnull String inputDhtName, @Nonnull String destDhtName);

}
