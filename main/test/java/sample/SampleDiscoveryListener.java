package sample;

import gridool.GridNode;
import gridool.discovery.DiscoveryEvent;
import gridool.discovery.GridDiscoveryListener;

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

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public class SampleDiscoveryListener implements GridDiscoveryListener {

    public SampleDiscoveryListener() {
        System.err.println("TestListener is initialized");
    }

    public void onChannelClosed() {
        System.err.println("channel closed");
    }

    public void onDiscovery(DiscoveryEvent event, GridNode node) {
        System.err.println(event + " - " + node);
        switch(event) {
            case join:
                break;
            case metricsUpdate:
                System.out.println(node.getMetrics());
                break;
            default:
                break;
        }
    }

}
