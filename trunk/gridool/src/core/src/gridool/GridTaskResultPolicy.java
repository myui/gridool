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
package gridool;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public enum GridTaskResultPolicy {

    /* Wait for all tasks are received. */
    CONTINUE,
    /* continue with running speculative tasks */
    CONTINUE_WITH_SPECULATIVE_TASKS,
    /* Proceeds the task without waiting other responses. */
    RETURN,
    /* Proceeds the task without waiting other responses. Then, the remaining tasks are canceled. */
    CANCEL_RETURN,
    /* Fail-over the task and execute it on another node. */
    FAILOVER,
    /* Task already processed. Then, skip it. */
    SKIP;

    private GridTaskResultPolicy() {}

}
