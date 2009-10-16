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
package gridool.communication;

import javax.annotation.Nonnull;

import gridool.GridConfiguration;
import gridool.communication.listeners.TaskMessageListener;
import gridool.communication.transport.srv.TcpCommunicationService;
import gridool.taskqueue.GridTaskQueueManager;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class CommunicationServiceProvider {

    private CommunicationServiceProvider() {}

    public static GridCommunicationService createService(@Nonnull GridTaskQueueManager taskMgr, @Nonnull GridConfiguration config) {
        GridCommunicationService srv = new TcpCommunicationService(config);
        srv.addListener(GridTopic.TASK, new TaskMessageListener(srv, taskMgr));
        return srv;
    }

}
