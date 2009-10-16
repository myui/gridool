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
package gridool.communication.messages;

import gridool.GridNode;
import gridool.communication.GridCommunicationMessage;
import gridool.communication.GridTopic;
import gridool.util.GridUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import xbird.util.io.IOUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public abstract class GridTaskMessage implements GridCommunicationMessage, Externalizable {
    private static final long serialVersionUID = -3765358775653601657L;

    @Nonnull
    private/* final */String taskId;
    @Nullable
    private GridNode senderNode;

    public GridTaskMessage() {}// for Externalizable

    public GridTaskMessage(@CheckForNull String taskId) {
        if(taskId == null) {
            throw new IllegalArgumentException();
        }
        this.taskId = taskId;
    }

    public GridTopic getTopic() {
        return GridTopic.TASK;
    }

    public String getMessageId() {
        return taskId + '@' + senderNode;
    }

    @Nonnull
    public String getTaskId() {
        return taskId;
    }

    public String getJobId() {
        return GridUtils.extractJobIdFromTaskId(taskId);
    }

    public GridNode getSenderNode() {
        return senderNode;
    }

    public void setSenderNode(GridNode node) {
        this.senderNode = node;
    }

    @Override
    public String toString() {
        return getMessageId();
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.taskId = IOUtils.readString(in);
        this.senderNode = (GridNode) in.readObject();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(taskId, out);
        out.writeObject(senderNode);
    }

}
