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

import java.io.Serializable;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public interface GridNodeMetrics extends Serializable {

    public long getLastUpdateTime();

    // -------------------------------
    // node information

    public int getAvailableProcessors();

    public long getUptime();

    // ---------------------
    // resource metrics

    public float getLastCpuLoad();

    public float getAverageCpuLoad();

    public float getMaximumCpuLoad();

    public double getLastIoWait();

    public double getAverageIoWait();

    public long getHeapMemoryFree();

    public long getAvailableDiskSpace();

    // ---------------------
    // task metrics

    public int getLastActiveTasks();

    public float getAverageActiveTasks();

    public int getMaximumActiveTasks();

    public int getLastPassiveTasks();

    public float getAveragePassiveTasks();

    public int getMaximumPassiveTasks();

    public int getTotalCanceledTasks();
    
    public int getTotalStealedTasks();

    public long getLastTaskExecutionTime();

    public double getAverageTaskExecutionTime();

    public long getMaximumTaskExecutionTime();

    public long getLastTaskWaitTime();

    public double getAverageTaskWaitTime();

    public long getMaximumTaskWaitTime();
}
