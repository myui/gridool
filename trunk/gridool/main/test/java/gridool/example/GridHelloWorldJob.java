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
package gridool.example;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import gridool.GridException;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.construct.GridJobSplitAdapter;
import gridool.construct.GridTaskAdapter;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridHelloWorldJob extends GridJobSplitAdapter<String, Integer> {
    private static final long serialVersionUID = 5430297811056376479L;

    private transient List<Integer> results;

    public GridHelloWorldJob() {}

    @SuppressWarnings("serial")
    @Override
    protected Collection<? extends GridTask> split(int gridSize, String phrase)
            throws GridException {
        String[] words = phrase.split(" ");
        List<GridTask> tasks = new ArrayList<GridTask>(words.length);
        this.results = new ArrayList<Integer>(words.length);
        for(final String word : words) {
            tasks.add(new GridTaskAdapter(this, true) {
                public Serializable execute() throws GridException {                    
                    return word.length();
                }
            });
        }
        return tasks;
    }

    public GridTaskResultPolicy result(GridTaskResult result) throws GridException {
        Integer res = result.getResult();
        results.add(res);
        return GridTaskResultPolicy.CONTINUE;
    }

    public Integer reduce() throws GridException {
        int sum = 0;
        for(Integer i : results) {
            sum += i.intValue();
        }
        return sum;
    }

}
