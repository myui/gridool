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

import gridool.util.io.IOUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import javax.annotation.Nonnull;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridTaskRequestMessage extends GridTaskMessage {
    private static final long serialVersionUID = -2082080206712697142L;

    @Nonnull
    private/* final */byte[] task;

    public GridTaskRequestMessage() {// for Externalizable
        super();
    }

    public GridTaskRequestMessage(@Nonnull String taskId, @Nonnull byte[] task) {
        super(taskId);
        this.task = task;
    }

    public GridMessageType getMessageType() {
        return GridMessageType.taskRequest;
    }

    public byte[] getMessage() {
        return task;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.task = IOUtils.readBytes(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeBytes(task, out);
    }

}
