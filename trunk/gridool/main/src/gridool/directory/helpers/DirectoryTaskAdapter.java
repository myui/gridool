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
package gridool.directory.helpers;

import gridool.GridException;
import gridool.GridJob;
import gridool.annotation.GridDirectoryResource;
import gridool.construct.GridTaskAdapter;
import gridool.directory.ILocalDirectory;
import gridool.directory.ops.DirectoryOperation;

import java.io.Serializable;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class DirectoryTaskAdapter extends GridTaskAdapter {
    private static final long serialVersionUID = -8535835556419815643L;

    private final DirectoryOperation ops;

    @GridDirectoryResource
    private transient ILocalDirectory directory;

    @SuppressWarnings("unchecked")
    public DirectoryTaskAdapter(GridJob job, DirectoryOperation ops) {
        super(job, false);
        this.ops = ops;
    }

    @Override
    public boolean injectResources() {
        return true;
    }

    @Override
    public boolean isAsyncTask() {
        return ops.isAsyncOps();
    }

    protected Serializable execute() throws GridException {
        if(directory == null) {
            throw new IllegalStateException();
        }
        return ops.execute(directory);
    }

}
