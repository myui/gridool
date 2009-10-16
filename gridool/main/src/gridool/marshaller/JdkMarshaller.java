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
public final class JdkMarshaller implements GridMarshaller {

    public JdkMarshaller() {}

    public byte[] marshall(Serializable obj) throws GridException {
        try {
            return ObjectUtils.toBytes(obj);
        } catch (Throwable e) {
            LogFactory.getLog(getClass()).error(e.getMessage(), e);
            throw new GridException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends Serializable> T unmarshall(byte[] obj, ClassLoader cl) throws GridException {
        if(cl == null) {
            cl = Thread.currentThread().getContextClassLoader();
        }
        final FastByteArrayInputStream bis = new FastByteArrayInputStream(obj);
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
