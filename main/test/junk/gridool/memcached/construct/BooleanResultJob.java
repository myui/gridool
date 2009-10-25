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
package gridool.memcached.construct;

import gridool.GridException;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class BooleanResultJob extends MemcachedJob<Boolean> {
    private static final long serialVersionUID = 3827577658728944676L;

    private boolean success = true;

    public BooleanResultJob() {
        super();
    }

    @Override
    public final GridTaskResultPolicy result(GridTask task, GridTaskResult result)
            throws GridException {
        final Boolean res = result.getResult();
        if(res == null) {
            success = false;
        } else {
            if(res.booleanValue() && isReadOnlyOps()) { // immediate return
                this.success = true;
                return GridTaskResultPolicy.RETURN;
            } else {
                success = false;
            }
        }
        return GridTaskResultPolicy.CONTINUE;
    }

    @Override
    public final Boolean reduce() throws GridException {
        return success ? Boolean.TRUE : Boolean.FALSE;
    }

}
