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
package gridool.db;

import gridool.GridException;
import gridool.GridJob;
import gridool.GridNode;
import gridool.GridResourceRegistry;
import gridool.annotation.GridRegistryResource;
import gridool.construct.GridTaskAdapter;

import java.io.Serializable;
import java.sql.SQLException;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class DBTaskAdapter extends GridTaskAdapter {
    private static final long serialVersionUID = -3044319526320978160L;

    @Nonnull
    private final DBOperation opr;

    private boolean injectResources = false;
    @GridRegistryResource
    private transient GridResourceRegistry registry;

    @SuppressWarnings("unchecked")
    public DBTaskAdapter(@Nonnull GridJob job, @CheckForNull DBOperation opr) {
        super(job, false);
        if(opr == null) {
            throw new IllegalArgumentException("DBOperation must be specified");
        }
        this.opr = opr;
    }

    @Override
    public boolean injectResources() {
        return injectResources;
    }

    @Override
    public boolean isReplicatable() {
        return opr.isReplicatable();
    }

    @Override
    public void setTransferToReplica(GridNode masterNode) {
        opr.setTransferToReplica(masterNode);
        this.injectResources = true;
    }

    protected Serializable execute() throws GridException {
        if(injectResources) {
            assert (registry != null);
            opr.setResourceRegistry(registry);
        }
        try {
            return opr.execute();
        } catch (SQLException e) {
            throw new GridException(e.getMessage());
        }
    }

}
