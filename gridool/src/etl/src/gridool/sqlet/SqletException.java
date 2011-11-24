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
package gridool.sqlet;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class SqletException extends Exception {
    private static final long serialVersionUID = -5155895009547145981L;

    private final SqletErrorType type;

    public SqletException(SqletErrorType err) {
        this.type = err;
    }

    public SqletException(SqletErrorType err, String message) {
        super(message);
        this.type = err;
    }

    public SqletException(SqletErrorType err, Throwable cause) {
        super(cause);
        this.type = err;
    }

    public SqletException(SqletErrorType err, String message, Throwable cause) {
        super(message, cause);
        this.type = err;
    }

    public SqletErrorType getErrorType() {
        return type;
    }

    public enum SqletErrorType {
        parseError, execFailed, configFailed, unsupported;
    }
}
