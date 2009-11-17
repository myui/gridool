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
package gridool.mapred.dht;

import gridool.GridJob;
import gridool.GridTask;
import gridool.mapred.dht.task.DhtMapShuffleTask;

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
public abstract class DhtMapReduceJobConf implements Serializable {
    private static final long serialVersionUID = -6387687169104905041L;
    public static final String OutputKeyCollectionName = "gridool#output-key-collections";

    @Nonnull
    protected String inputTableName;

    public DhtMapReduceJobConf(@CheckForNull String inputTblName) {
        checkTableName(inputTblName);
        this.inputTableName = inputTblName;
    }

    private static void checkTableName(final String tblName) {
        if(tblName == null) {
            throw new IllegalArgumentException();
        }
        if(tblName.startsWith("gridool#")) {
            throw new IllegalArgumentException("DHT name starting with 'gridool#' is reserved: "
                    + tblName);
        }
    }

    public String getInputTableName() {
        return inputTableName;
    }

    /**
     * Used by internal system only.
     */
    void setInputTableName(@Nonnull String inputTableName) {
        this.inputTableName = inputTableName;
    }

    public String getOutputTableName() {
        return null;
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
    protected abstract DhtMapShuffleTask makeMapShuffleTask(@Nonnull GridJob job, @Nonnull String inputTableName, @Nonnull String destTableName);

    @SuppressWarnings("unchecked")
    @Nullable
    protected abstract GridTask makeReduceTask(@Nonnull GridJob job, @Nonnull String inputTableName, @Nonnull String destTableName);

}
