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
package gridool;

import gridool.util.io.IOUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import javax.annotation.Nonnull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridJobDesc implements Externalizable {
    private static final long serialVersionUID = -99505792234098119L;

    public static final String DEFAULT_DEPLOYMENT_GROUP = "default";

    @Nonnull
    private/* final */String deploymentGroup;
    @Nonnull
    private/* final */String jobClass;

    public GridJobDesc() {} // for Externalizable

    public GridJobDesc(@Nonnull Class<? extends GridJob<?, ?>> jobClass) {
        this(jobClass.getName(), DEFAULT_DEPLOYMENT_GROUP);
    }

    public GridJobDesc(@Nonnull String jobClass) {
        this(jobClass, DEFAULT_DEPLOYMENT_GROUP);
    }

    public GridJobDesc(@Nonnull Class<? extends GridJob<?, ?>> jobClass, @Nonnull String deploymentGroup) {
        this(jobClass.getName(), deploymentGroup);
    }

    public GridJobDesc(@Nonnull String jobClass, @Nonnull String deploymentGroup) {
        this.jobClass = jobClass;
        this.deploymentGroup = deploymentGroup;
    }

    @Nonnull
    public String getDeploymentGroup() {
        return deploymentGroup;
    }

    @Nonnull
    public String getJobClass() {
        return jobClass;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.deploymentGroup = IOUtils.readString(in);
        this.jobClass = IOUtils.readString(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(deploymentGroup, out);
        IOUtils.writeString(jobClass, out);
    }

}
