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
package gridool.marshaller;

import gridool.GridException;

import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.Nonnull;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public abstract class ProtobufMarshaller extends MarshallerBase<Message> {

    public ProtobufMarshaller() {}

    @Override
    public <T extends Message> byte[] marshall(final T obj) throws GridException {
        return obj.toByteArray();
    }

    public <T extends Message> void marshall(final T obj, final OutputStream out)
            throws GridException {
        try {
            obj.writeTo(out);
        } catch (IOException e) {
            throw new GridException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends Message> T unmarshall(final byte[] ary, final ClassLoader cl)
            throws GridException {
        final Builder builder;
        try {
            builder = createBuilder().mergeFrom(ary);
        } catch (InvalidProtocolBufferException e) {
            throw new GridException(e);
        }
        return (T) builder.build();
    }

    /**
     * Override this method for efficiency.
     */
    @Nonnull
    protected Builder createBuilder() {
        Message obj = createObject();
        return obj.newBuilderForType();
    }

}
