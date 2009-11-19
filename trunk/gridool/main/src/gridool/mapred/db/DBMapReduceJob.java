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
package gridool.mapred.db;

import gridool.GridException;
import gridool.GridJobFuture;
import gridool.GridKernel;
import gridool.annotation.GridKernelResource;

import java.util.concurrent.ExecutionException;

import org.apache.commons.logging.LogFactory;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class DBMapReduceJob extends DBMapJob {
    private static final long serialVersionUID = -8756855076582206285L;

    @GridKernelResource
    private transient GridKernel kernel;

    public DBMapReduceJob() {}

    @Override
    public boolean injectResources() {
        return true;
    }

    @Override
    public String reduce() throws GridException {
        final GridJobFuture<String> result = kernel.execute(DBReduceJob.class, jobConf);
        try {
            return result.get();
        } catch (InterruptedException ie) {
            LogFactory.getLog(getClass()).error(ie.getMessage(), ie);
            throw new GridException(ie);
        } catch (ExecutionException ee) {
            LogFactory.getLog(getClass()).error(ee.getMessage(), ee);
            throw new GridException(ee);
        }
    }

}
