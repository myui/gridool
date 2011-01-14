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

import gridool.GridConfiguration;
import gridool.routing.strategy.ConsistentHashRouter;

import javax.annotation.Nonnull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class GridRouterFactory {

    private GridRouterFactory() {}

    @Nonnull
    public static GridRouter createRouter(@Nonnull GridConfiguration config) {
        //final String factoryName = Settings.get("gridool.router.algorithm");
        //if(ConsistentHashRouter.class.getSimpleName().equalsIgnoreCase(factoryName)) {
        //    return new ConsistentHashRouter(config);
        //}
        GridRouter router = new ConsistentHashRouter(config);
        return router;
    }

}
