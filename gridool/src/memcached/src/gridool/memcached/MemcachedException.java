/*
 * @(#)$Id$
 *
 * Copyright 2009-2010 Makoto YUI
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

import gridool.memcached.binary.BinaryProtocol.ResponseStatus;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class MemcachedException extends Exception {
    private static final long serialVersionUID = -6941352118810069540L;

    private final ResponseStatus errCode;

    public MemcachedException(ResponseStatus errCode) {
        super(errCode.getMessage());
        this.errCode = errCode;
    }

    public MemcachedException(ResponseStatus errCode, String message) {
        super(message);
        this.errCode = errCode;
    }

    public MemcachedException(ResponseStatus errCode, Throwable cause) {
        super(errCode.getMessage(), cause);
        this.errCode = errCode;
    }

    public MemcachedException(ResponseStatus errCode, String message, Throwable cause) {
        super(message, cause);
        this.errCode = errCode;
    }

    public ResponseStatus getErrorCode() {
        return errCode;
    }

}
