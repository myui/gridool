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
package gridool.memcached;

import java.io.Serializable;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class CASValue<T extends Serializable> implements Serializable {
    private static final long serialVersionUID = 4364869448526397975L;
    
    private final long casId;
    private final T value;

    public CASValue(long casId, T value) {
        this.casId = casId;
        this.value = value;
    }

    public long getCasId() {
        return casId;
    }

    public T getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value == null ? null : (value.toString() + '(' + casId + ')');
    }

}
