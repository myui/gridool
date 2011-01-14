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
import gridool.util.io.FastMultiByteArrayOutputStream;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public abstract class MarshallerBase<BASE_TYPE> implements GridMarshaller<BASE_TYPE> {

    public MarshallerBase() {}

    @Override
    public <T extends BASE_TYPE> byte[] marshall(T obj) throws GridException {
        FastMultiByteArrayOutputStream bos = new FastMultiByteArrayOutputStream();
        marshall(obj, bos);
        return bos.toByteArray();
    }

    @Override
    public <T extends BASE_TYPE> T unmarshall(byte[] ary) throws GridException {
        return unmarshall(ary, null);
    }
}
