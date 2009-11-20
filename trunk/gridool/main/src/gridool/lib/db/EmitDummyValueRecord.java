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
package gridool.lib.db;

import gridool.GridException;
import gridool.marshaller.GridMarshaller;

import java.io.OutputStream;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class EmitDummyValueRecord extends GenericDBRecord {
    private static final long serialVersionUID = -2852866152889505711L;

    public EmitDummyValueRecord() {}

    public EmitDummyValueRecord(byte[] key, Object... results) {
        super(key, results);
    }

    public EmitDummyValueRecord(byte[] key, Object[] results, int[] columnTypes) {
        super(key, results, columnTypes);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void writeTo(GridMarshaller marshaller, OutputStream out) throws GridException {
        marshaller.marshall(1, out);
    }

}
