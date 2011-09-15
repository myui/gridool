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
package gridool.processors;

import javax.annotation.CheckForNull;

import gridool.GridConfiguration;
import gridool.GridResourceRegistry;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public abstract class AbstractGridProcessor implements GridProcessor {

    protected final GridConfiguration config;
    protected final GridResourceRegistry resourceRegistry;

    public AbstractGridProcessor(@CheckForNull GridResourceRegistry resourceRegistry, @CheckForNull GridConfiguration config) {
        assert (resourceRegistry != null);
        assert (config != null);
        this.resourceRegistry = resourceRegistry;
        this.config = config;
    }

}
