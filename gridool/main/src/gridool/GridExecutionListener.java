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

import java.util.EventListener;

import javax.annotation.Nonnull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public interface GridExecutionListener extends EventListener {

    void onJobStarted(@Nonnull GridJob<?, ?> job);

    void onJobFinished(@Nonnull GridJob<?, ?> job);

    void onTaskStarted(@Nonnull GridTask task);

    /**
     * @param progress use -1f to use approximate report. 
     *  Otherwise use value between 0 and 1.
     */
    void progress(@Nonnull GridTask task, float progress);

    void onTaskFinished(@Nonnull GridTask task);
    
    void onTaskStealed(@Nonnull GridTask task);
    
    void onTaskCanceled(@Nonnull GridTask task);

}
