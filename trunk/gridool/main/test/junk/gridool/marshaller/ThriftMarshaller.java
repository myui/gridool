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

import java.io.OutputStream;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

import xbird.util.io.FastByteArrayInputStream;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
@SuppressWarnings("unchecked")
public abstract class ThriftMarshaller extends MarshallerBase<TBase> {

    public ThriftMarshaller() {}

    public <T extends TBase> void marshall(final T obj, final OutputStream out)
            throws GridException {
        TIOStreamTransport trans = new TIOStreamTransport(out);
        final TCompactProtocol prot = new TCompactProtocol(trans);
        try {
            obj.write(prot);
        } catch (TException e) {
            throw new GridException(e);
        }
    }

    public <T extends TBase> T unmarshall(final byte[] ary, final ClassLoader cl)
            throws GridException {
        FastByteArrayInputStream bais = new FastByteArrayInputStream(ary);
        TIOStreamTransport trans = new TIOStreamTransport(bais);
        final TCompactProtocol prot = new TCompactProtocol(trans);
        final T obj = createObject();
        try {
            obj.read(prot);
        } catch (TException e) {
            throw new GridException(e);
        }
        return obj;
    }

}
