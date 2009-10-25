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
package sample;

import gridool.GridExecutionListener;
import gridool.GridJob;
import gridool.GridTask;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public class SampleExecListener implements GridExecutionListener {

    public SampleExecListener() {
        System.out.println("SampleExecListener initilized");
    }

    public void onJobStarted(GridJob<?, ?> job) {
        System.err.println("job started: " + job.getJobId());
    }

    public void onJobFinished(GridJob<?, ?> job) {
        System.err.println("job finished: " + job.getJobId());
    }

    public void onTaskStarted(GridTask task) {
        System.err.println("task started: " + task.getTaskId());
    }

    public void onTaskFinished(GridTask task) {
        System.err.println("task finished: " + task.getTaskId());
    }

    public void progress(GridTask task, float progress) {
        System.out.println("task '" + task.getTaskId() + "' progressed.. "
                + String.format("%.2f", progress) + '%');
    }

    public void onTaskCanceled(GridTask task) {
        System.err.println("task canceled: " + task.getTaskId());
    }

    public void onTaskStealed(GridTask task) {
        System.err.println("task stealed: " + task.getTaskId());
    }

}
