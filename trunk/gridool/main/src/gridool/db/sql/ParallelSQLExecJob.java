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
package gridool.db.sql;

import gridool.GridException;
import gridool.GridNode;
import gridool.GridResourceRegistry;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.annotation.GridRegistryResource;
import gridool.construct.GridJobBase;
import gridool.db.catalog.DistributionCatalog;
import gridool.routing.GridTaskRouter;
import gridool.util.GridUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import xbird.util.io.IOUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class ParallelSQLExecJob extends GridJobBase<ParallelSQLExecJob.JobConf, String> {
    private static final long serialVersionUID = -3258710936720234846L;

    @GridRegistryResource
    private transient GridResourceRegistry registry;

    public ParallelSQLExecJob() {
        super();
    }

    @Override
    public boolean injectResources() {
        return true;
    }

    public Map<GridTask, GridNode> map(GridTaskRouter router, JobConf jobConf) throws GridException {
        // phase #1 preparation
        DistributionCatalog catalog = registry.getDistributionCatalog();
        SQLTranslator translator = new SQLTranslator(catalog);
        String mapQuery = translator.translateQuery(jobConf.mapQuery);
        GridNode[] masters = catalog.getMasters(DistributionCatalog.defaultDistributionKey);
        runPreparation(mapQuery, masters, jobConf.retTableName);

        return null;
    }

    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        return null;
    }

    public String reduce() throws GridException {
        return null;
    }

    private static void runPreparation(final String mapQuery, final GridNode[] masters, final String retTableName) {
        String prepareQuery = constructQuery(mapQuery, masters, retTableName);
        
    }

    private static String constructQuery(final String mapQuery, final GridNode[] masters, final String retTableName) {
        if(masters.length == 0) {
            throw new IllegalArgumentException();
        }
        final StringBuilder buf = new StringBuilder(512);
        buf.append("CREATE VIEW ");
        final String tmpViewName = "tmp" + retTableName;
        buf.append(tmpViewName);
        buf.append(" AS (\n");
        buf.append(mapQuery);
        buf.append(");\n");
        final int numTasks = masters.length;
        for(int i = 0; i < numTasks; i++) {
            buf.append("CREATE TABLE ");
            buf.append(tmpViewName);
            buf.append("task");
            buf.append(i);
            buf.append(" (LIKE ");
            buf.append(tmpViewName);
            buf.append(");\n");
        }
        buf.append("CREATE VIEW ");
        buf.append(retTableName);
        buf.append(" AS (\n");
        final int lastTask = numTasks - 1;
        for(int i = 0; i < lastTask; i++) {
            buf.append("SELECT * FROM ");
            buf.append(tmpViewName);
            buf.append("task");
            buf.append(i);
            buf.append(" UNION ALL \n");
        }
        buf.append("SELECT * FROM ");
        buf.append(tmpViewName);
        buf.append("task");
        buf.append(lastTask);
        buf.append("\n);");
        return buf.toString();
    }

    public static final class JobConf implements Externalizable {

        @Nonnull
        private String retTableName;
        @Nonnull
        private String mapQuery;
        @Nonnull
        private String reduceQuery;

        public JobConf() {}//Externalizable

        public JobConf(@Nonnull String mapQuery, @Nonnull String reduceQuery) {
            this.retTableName = GridUtils.generateQueryName();
            this.mapQuery = mapQuery;
            this.reduceQuery = reduceQuery;
        }

        public JobConf(@Nullable String retTableName, @Nonnull String mapQuery, @Nonnull String reduceQuery) {
            this.retTableName = (retTableName == null) ? GridUtils.generateQueryName()
                    : retTableName;
            this.mapQuery = mapQuery;
            this.reduceQuery = reduceQuery;
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.retTableName = IOUtils.readString(in);
            this.mapQuery = IOUtils.readString(in);
            this.reduceQuery = IOUtils.readString(in);
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            IOUtils.writeString(retTableName, out);
            IOUtils.writeString(mapQuery, out);
            IOUtils.writeString(reduceQuery, out);
        }

    }

}