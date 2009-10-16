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
import gridool.GridException;
import gridool.GridJob;
import gridool.GridNode;
import gridool.GridTask;
import gridool.GridTaskResult;
import gridool.GridTaskResultPolicy;
import gridool.construct.GridJobBase;
import gridool.construct.GridTaskAdapter;
import gridool.routing.GridTaskRouter;

import java.io.Serializable;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import xbird.util.string.StringUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public class Sample1 extends GridJobBase<String, String> {

    private static final long serialVersionUID = 5354344187791826391L;

    private String aggregate = "1111111";

    public Sample1() {
        super();
    }

    public Map<GridTask, GridNode> map(GridTaskRouter router, String arg) throws GridException {
        String[] args = arg.split(" ");
        final Map<GridTask, GridNode> map = new IdentityHashMap<GridTask, GridNode>();
        for(final String phrase : args) {
            GridTask key = new Sample1Task(this, false, phrase);
            List<GridNode> nodes = router.selectNodes(StringUtils.getBytes(phrase));
            GridNode node = nodes.get(0);
            map.put(key, node);
        }
        return map;
    }

    public GridTaskResultPolicy result(GridTask task, GridTaskResult result) throws GridException {
        String phrase = result.getResult();
        aggregate += phrase;
        return GridTaskResultPolicy.CONTINUE;
    }

    public String reduce() throws GridException {
        return aggregate;
    }
    
    private final class Sample1Task extends GridTaskAdapter {
        private static final long serialVersionUID = -5358475010641915459L;
        
        private final String phrase;

        @SuppressWarnings("unchecked")
        private Sample1Task(GridJob job, boolean failover, String phrase) {
            super(job, failover);
            this.phrase = phrase;
        }

        public Serializable execute() throws GridException {
            return phrase;
        }
    }

}
