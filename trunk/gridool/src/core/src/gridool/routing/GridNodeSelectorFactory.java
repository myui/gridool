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
package gridool.routing;

import gridool.Settings;
import gridool.routing.selector.LoadBalancingNodeSelector;
import gridool.routing.selector.PrimaryNodeSelector;

import org.apache.commons.logging.LogFactory;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridNodeSelectorFactory {

    private GridNodeSelectorFactory() {}

    public static GridNodeSelector createSelector() {
        final String selectorName = Settings.get("gridool.router.nodeselector");
        if(PrimaryNodeSelector.class.getName().equals(selectorName)) {
            return new PrimaryNodeSelector();
        } else if(LoadBalancingNodeSelector.class.getName().equals(selectorName)) {
            return new LoadBalancingNodeSelector();
        } else {
            LogFactory.getLog(GridNodeSelectorFactory.class).warn("GridNodeSelector '"
                    + selectorName + "' not found. Use the default PrimaryNodeSelector.");
        }
        return new PrimaryNodeSelector();
    }
}
