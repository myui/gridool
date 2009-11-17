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
import java.io.Serializable;

import org.apache.commons.logging.LogFactory;

import xbird.util.io.CustomObjectInputStream;
import xbird.util.io.FastByteArrayInputStream;
import xbird.util.lang.ObjectUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class JdkMarshaller extends MarshallerBase<Serializable> {

    public JdkMarshaller() {}

    public <T extends Serializable> T createObject() {
        throw new UnsupportedOperationException();
    }

    public <T extends Serializable> void marshall(T obj, OutputStream out) throws GridException {
        try {
            ObjectUtils.toStream(obj, out);
        } catch (Throwable e) {
            LogFactory.getLog(getClass()).error(e.getMessage(), e);
            throw new GridException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends Serializable> T unmarshall(byte[] ary, ClassLoader cl) throws GridException {
        if(cl == null) {
            cl = Thread.currentThread().getContextClassLoader();
        }
        final FastByteArrayInputStream bis = new FastByteArrayInputStream(ary);
        final Object result;
        try {
            CustomObjectInputStream ois = new CustomObjectInputStream(bis, cl);
            result = ois.readObject();
        } catch (IOException e) {
            LogFactory.getLog(getClass()).error(e.getMessage(), e);
            throw new GridException(e);
        } catch (ClassNotFoundException e) {
            LogFactory.getLog(getClass()).error(e.getMessage(), e);
            throw new GridException(e);
        }
        return (T) result;
    }

}
