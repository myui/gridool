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
package gridool.replication;

import gridool.communication.payload.GridNodeInfo;
import gridool.routing.GridTaskRouter;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public abstract class ReplicaSelectorBase implements ReplicaSelector {

    @Nonnull
    protected final GridNodeInfo localNode;
    @Nonnull
    protected final GridTaskRouter router;

    public ReplicaSelectorBase(@CheckForNull GridNodeInfo localNode, @CheckForNull GridTaskRouter router) {
        if(localNode == null) {
            throw new IllegalArgumentException();
        }
        if(router == null) {
            throw new IllegalArgumentException();
        }
        this.localNode = localNode;
        this.router = router;
    }

}
