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
package gridool.construct;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.CheckForNull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gridool.GridException;
import gridool.GridJob;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public abstract class GridListAggregationJob<A, R> extends GridJobDelegate<A, R> {
    private static final long serialVersionUID = -3268221629403144014L;
    protected static final Log LOG = LogFactory.getLog(GridListAggregationJob.class);

    protected transient final List<Serializable> results; // TODO REVIEWME should be stateless?

    public GridListAggregationJob(@CheckForNull GridJob<A, R> job, int jobsize) {
        this(job, new ArrayList<Serializable>(jobsize));
    }

    public GridListAggregationJob(@CheckForNull GridJob<A, R> job, @CheckForNull List<Serializable> holder) {
        super(job);
        if(holder == null) {
            throw new IllegalArgumentException();
        }
        this.results = holder;
    }

    @Override
    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        final Serializable res = result.getResult();
        if(res == null) {
            if(result.isFailoverScheduled()) {
                LOG.info("Failover is scheduled for task: " + result.getTaskId());
                return GridTaskResultPolicy.FAILOVER;
            }
            final GridException exception = result.getException();
            if(exception == null) {
                throw new GridException("Both result and exception are not set for task: "
                        + result.getTaskId());
            } else {
                throw exception;
            }
        } else {
            results.add(res);
            return GridTaskResultPolicy.CONTINUE;
        }
    }

    @Override
    public final R reduce() throws GridException {
        return doReduce(results);
    }

    protected abstract R doReduce(List<Serializable> results);

}
