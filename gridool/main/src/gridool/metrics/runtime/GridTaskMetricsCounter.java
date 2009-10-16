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
package gridool.metrics.runtime;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class GridTaskMetricsCounter {

    private int startedTasks = 0;
    private int activeTasks = 0;
    private int finishedTasks = 0;
    private int stealedTasks = 0;
    private int calceledTasks = 0;

    private long totalExecutionTime = 0L;
    private long maxExecutionTime = 0L;
    private long totalWaitTime = 0L;
    private long maxWaitTime = 0L;

    public GridTaskMetricsCounter() {}

    public synchronized void taskCreated() {
        startedTasks++;
    }

    public synchronized void taskStealed(long waitTime) {
        startedTasks--;
        stealedTasks++;
        totalWaitTime += waitTime;
        if(waitTime > maxWaitTime) {
            this.maxWaitTime = waitTime;
        }
    }

    public synchronized void taskStarted(long waitTime) {
        activeTasks++;
        totalWaitTime += waitTime;
        if(waitTime > maxWaitTime) {
            this.maxWaitTime = waitTime;
        }
    }

    public synchronized void taskCanceled(long execTime) {
        activeTasks--;
        calceledTasks++;
        //totalExecutionTime += execTime;
    }

    public synchronized void taskFinished(long execTime) {
        activeTasks--;
        finishedTasks++;
        totalExecutionTime += execTime;
        if(execTime > maxExecutionTime) {
            this.maxExecutionTime = execTime;
        }
    }

    public synchronized int getCurrentActiveTasks() {
        return activeTasks;
    }

    public synchronized int getCurrentPassiveTasks() {
        return startedTasks - finishedTasks - activeTasks - stealedTasks;
    }

    public synchronized int getCurrentStealedTasks() {
        return stealedTasks;
    }

    public synchronized int getCurrentCanceledTasks() {
        return calceledTasks;
    }

    public synchronized long getAverageExecutionTime() {
        return finishedTasks == 0 ? 0L : (totalExecutionTime / finishedTasks);
    }

    public synchronized long getAverageWaitTime() {
        int waitingTasks = startedTasks - finishedTasks - activeTasks - stealedTasks;
        return waitingTasks == 0 ? 0L : (totalWaitTime / waitingTasks);
    }

    public synchronized long getMaxExecutionTime() {
        return maxExecutionTime;
    }

    public synchronized long getMaxWaitTime() {
        return maxWaitTime;
    }

}
