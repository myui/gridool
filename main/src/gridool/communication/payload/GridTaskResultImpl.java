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
package gridool.communication.payload;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridTaskResult;

import java.io.Serializable;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridTaskResultImpl implements GridTaskResult {
    private static final long serialVersionUID = -4203147223987810971L;

    private final String taskId;
    private GridNode executedNode;

    @Nullable
    private final Serializable result;
    @Nullable
    private final GridException exception;

    private boolean failoverScheduled = false;

    public GridTaskResultImpl(@Nonnull String taskId, @Nullable Serializable result) {
        this.taskId = taskId;
        this.result = result;
        this.exception = null;
    }

    public GridTaskResultImpl(@Nonnull String taskId, @CheckForNull GridException exception) {
        if(exception == null) {
            throw new IllegalArgumentException();
        }
        this.taskId = taskId;
        this.result = null;
        this.exception = exception;
    }

    public GridTaskResultImpl(@Nonnull String taskId, @Nonnull GridNode executedNode, @Nullable GridException exception) {
        this.taskId = taskId;
        this.executedNode = executedNode;
        this.result = null;
        this.exception = exception;
    }

    public String getTaskId() {
        return taskId;
    }

    public GridNode getExecutedNode() {
        return executedNode;
    }

    @SuppressWarnings("unchecked")
    public <T extends Serializable> T getResult() {
        return (T) result;
    }

    public GridException getException() {
        return exception;
    }

    public GridTaskResult scheduleFailover() {
        this.failoverScheduled = true;
        return this;
    }

    public boolean isFailoverScheduled() {
        return failoverScheduled;
    }

}
