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
package gridool;

import javax.annotation.Nonnull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public enum GridErrorDescription {

    GRID_SERVICE_NOT_FOUND("Required Grid service is not registered."),
    NODE_NOT_FOUND("There should be at least one node in the gird, but no node was detected.");

    @Nonnull
    private String message;

    private GridErrorDescription(String defaultMessage) {
        this.message = defaultMessage;
    }

    public GridErrorDescription message(String msg) {
        this.message = msg;
        return this;
    }

    public String getMessage() {
        return message;
    }
}
