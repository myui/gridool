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

import gridool.GridTaskResult;
import gridool.util.io.IOUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridTaskResponseMessage extends GridTaskMessage {
    private static final long serialVersionUID = -8561114374143574865L;

    @Nonnull
    private/* final */byte[] result;
    @Nullable
    private/* final */String deploymentGroup;

    public GridTaskResponseMessage() {// for Externalizable
        super();
    }

    public GridTaskResponseMessage(@Nonnull String taskId, @Nonnull byte[] taskResult, @Nullable String deploymentGroup) {
        super(taskId);
        this.result = taskResult;
        this.deploymentGroup = deploymentGroup;
    }

    @Override
    public GridMessageType getMessageType() {
        return GridMessageType.taskResponse;
    }

    /**
     * Get bytes of {@link GridTaskResult}.
     */
    @Override
    @Nonnull
    public byte[] getMessage() {
        return result;
    }

    @Nullable
    public String getDeploymentGroup() {
        return deploymentGroup;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.result = IOUtils.readBytes(in);
        this.deploymentGroup = IOUtils.readString(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeBytes(result, out);
        IOUtils.writeString(deploymentGroup, out);
    }

}
