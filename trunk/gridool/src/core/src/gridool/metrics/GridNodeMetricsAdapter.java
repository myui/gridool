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
package gridool.metrics;

import gridool.GridNodeMetrics;
import gridool.util.datetime.DateTimeFormatter;
import gridool.util.lang.PrintUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.text.DecimalFormat;
import java.util.Date;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridNodeMetricsAdapter implements GridNodeMetrics, Externalizable {
    private static final long serialVersionUID = 1089368704946377052L;

    private long lastUpdateTime;

    private int availableProcessors;
    private long uptime;

    private float lastCpuLoad;
    private float averageCpuLoad;
    private float maximumCpuLoad;
    private double lastIoWait;
    private double averageIoWait;
    private long heapMemoryFree;
    private long availableDiskSpace;

    private int lastActiveTasks;
    private float averageActiveTasks;
    private int maximumActiveTasks;
    private int lastPassiveTasks;
    private float averagePassiveTasks;
    private int maximumPassiveTasks;
    private int totalCanceledTasks;
    private int totalStealedTasks;
    private long lastTaskExecutionTime;
    private double averageTaskExecutionTime;
    private long maximumTaskExecutionTime;
    private long lastTaskWaitTime;
    private double averageTaskWaitTime;
    private long maximumTaskWaitTime;

    public GridNodeMetricsAdapter() {}// for Externalizable

    public GridNodeMetricsAdapter(long lastUpdateTime, int availableProcessors, long uptime, float lastCpuLoad, float averageCpuLoad, float maximumCpuLoad, double lastIoWait, double averageIoWait, long heapMemoryFree, long availableDiskSpace, int lastActiveTasks, float averageActiveTasks, int maximumActiveTasks, int lastPassiveTasks, float averagePassiveTasks, int maximumPassiveTasks, int totalCanceledTasks, int totalStealedTasks, long lastTaskExecutionTime, double averageTaskExecutionTime, long maximumTaskExecutionTime, long lastTaskWaitTime, double averageTaskWaitTime, long maximumTaskWaitTime) {
        this.lastUpdateTime = lastUpdateTime;
        this.availableProcessors = availableProcessors;
        this.uptime = uptime;
        this.lastCpuLoad = lastCpuLoad;
        this.averageCpuLoad = averageCpuLoad;
        this.maximumCpuLoad = maximumCpuLoad;
        this.lastIoWait = lastIoWait;
        this.averageIoWait = averageIoWait;
        this.heapMemoryFree = heapMemoryFree;
        this.availableDiskSpace = availableDiskSpace;
        this.lastActiveTasks = lastActiveTasks;
        this.averageActiveTasks = averageActiveTasks;
        this.maximumActiveTasks = maximumActiveTasks;
        this.lastPassiveTasks = lastPassiveTasks;
        this.averagePassiveTasks = averagePassiveTasks;
        this.maximumPassiveTasks = maximumPassiveTasks;
        this.totalCanceledTasks = totalCanceledTasks;
        this.totalStealedTasks = totalStealedTasks;
        this.lastTaskExecutionTime = lastTaskExecutionTime;
        this.averageTaskExecutionTime = averageTaskExecutionTime;
        this.maximumTaskExecutionTime = maximumTaskExecutionTime;
        this.lastTaskWaitTime = lastTaskWaitTime;
        this.maximumTaskWaitTime = maximumTaskWaitTime;
        this.averageTaskWaitTime = averageTaskWaitTime;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.lastUpdateTime = in.readLong();

        this.availableProcessors = in.readInt();
        this.uptime = in.readLong();

        this.lastCpuLoad = in.readFloat();
        this.averageCpuLoad = in.readFloat();
        this.maximumCpuLoad = in.readFloat();
        this.lastIoWait = in.readDouble();
        this.averageIoWait = in.readDouble();
        this.heapMemoryFree = in.readLong();
        this.availableDiskSpace = in.readLong();

        this.lastActiveTasks = in.readInt();
        this.averageActiveTasks = in.readFloat();
        this.maximumActiveTasks = in.readInt();
        this.lastPassiveTasks = in.readInt();
        this.averagePassiveTasks = in.readFloat();
        this.maximumPassiveTasks = in.readInt();
        this.totalCanceledTasks = in.readInt();
        this.totalStealedTasks = in.readInt();

        this.lastTaskExecutionTime = in.readLong();
        this.averageTaskExecutionTime = in.readDouble();
        this.maximumTaskExecutionTime = in.readLong();
        this.lastTaskWaitTime = in.readLong();
        this.averageTaskWaitTime = in.readDouble();
        this.maximumTaskWaitTime = in.readLong();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(lastUpdateTime);

        out.writeInt(availableProcessors);
        out.writeLong(uptime);

        out.writeFloat(lastCpuLoad);
        out.writeFloat(averageCpuLoad);
        out.writeFloat(maximumCpuLoad);
        out.writeDouble(lastIoWait);
        out.writeDouble(averageIoWait);
        out.writeLong(heapMemoryFree);
        out.writeLong(availableDiskSpace);

        out.writeInt(lastActiveTasks);
        out.writeFloat(averageActiveTasks);
        out.writeInt(maximumActiveTasks);
        out.writeInt(lastPassiveTasks);
        out.writeFloat(averagePassiveTasks);
        out.writeInt(maximumPassiveTasks);
        out.writeInt(totalCanceledTasks);
        out.writeInt(totalStealedTasks);

        out.writeLong(lastTaskExecutionTime);
        out.writeDouble(averageTaskExecutionTime);
        out.writeLong(maximumTaskExecutionTime);
        out.writeLong(lastTaskWaitTime);
        out.writeDouble(averageTaskWaitTime);
        out.writeLong(maximumTaskWaitTime);
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public int getAvailableProcessors() {
        return availableProcessors;
    }

    public long getUptime() {
        return uptime;
    }

    public float getLastCpuLoad() {
        return lastCpuLoad;
    }

    public float getAverageCpuLoad() {
        return averageCpuLoad;
    }

    public float getMaximumCpuLoad() {
        return maximumCpuLoad;
    }

    public double getLastIoWait() {
        return lastIoWait;
    }

    public double getAverageIoWait() {
        return averageIoWait;
    }

    public long getHeapMemoryFree() {
        return heapMemoryFree;
    }

    public long getAvailableDiskSpace() {
        return availableDiskSpace;
    }

    public int getLastActiveTasks() {
        return lastActiveTasks;
    }

    public float getAverageActiveTasks() {
        return averageActiveTasks;
    }

    public int getMaximumActiveTasks() {
        return maximumActiveTasks;
    }

    public int getLastPassiveTasks() {
        return lastPassiveTasks;
    }

    public float getAveragePassiveTasks() {
        return averagePassiveTasks;
    }

    public int getMaximumPassiveTasks() {
        return maximumPassiveTasks;
    }

    public int getTotalCanceledTasks() {
        return totalCanceledTasks;
    }

    public int getTotalStealedTasks() {
        return totalStealedTasks;
    }

    public long getLastTaskExecutionTime() {
        return lastTaskExecutionTime;
    }

    public double getAverageTaskExecutionTime() {
        return averageTaskExecutionTime;
    }

    public long getMaximumTaskExecutionTime() {
        return maximumTaskExecutionTime;
    }

    public long getLastTaskWaitTime() {
        return lastTaskWaitTime;
    }

    public double getAverageTaskWaitTime() {
        return averageTaskWaitTime;
    }

    public long getMaximumTaskWaitTime() {
        return maximumTaskWaitTime;
    }

    @Override
    public String toString() {
        final StringBuilder buf = new StringBuilder(512);
        final DecimalFormat f = new DecimalFormat("#,###");

        buf.append("\n----------------------------------------------------------------------------\n");
        buf.append(new Date());
        buf.append(" / lastUpdateTime: ");
        buf.append(new Date(lastUpdateTime));
        buf.append("\n>>> --------------------------");
        buf.append("\n availableProcessors: ");
        buf.append(availableProcessors);
        buf.append("\n uptime: ");
        buf.append(DateTimeFormatter.formatTime(uptime));
        buf.append("\n>>> --------------------------");
        buf.append("\n lastCpuLoad: ");
        buf.append(PrintUtils.toPercentString(lastCpuLoad));
        buf.append("\n averageCpuLoad: ");
        buf.append(PrintUtils.toPercentString(averageCpuLoad));
        buf.append("\n maximumCpuLoad: ");
        buf.append(PrintUtils.toPercentString(maximumCpuLoad));
        buf.append("\n lastIoWait: ");
        buf.append(PrintUtils.toPercentString(lastIoWait));
        buf.append("\n averageIoWait: ");
        buf.append(PrintUtils.toPercentString(averageIoWait));
        buf.append("\n heapMemoryFree: ");
        buf.append(f.format(heapMemoryFree));
        buf.append(" bytes\n availableDiskSpace: ");
        buf.append(f.format(availableDiskSpace));
        buf.append(" bytes");
        buf.append("\n>>> --------------------------");
        buf.append("\n lastActiveTasks: ");
        buf.append(lastActiveTasks);
        buf.append("\n averageActiveTasks: ");
        buf.append(averageActiveTasks);
        buf.append("\n maximumActiveTasks: ");
        buf.append(maximumActiveTasks);
        buf.append("\n lastPassiveTasks: ");
        buf.append(lastPassiveTasks);
        buf.append("\n averagePassiveTasks: ");
        buf.append(averagePassiveTasks);
        buf.append("\n maximumPassiveTasks: ");
        buf.append(maximumPassiveTasks);
        buf.append("\n totalCanceledTasks: ");
        buf.append(totalCanceledTasks);
        buf.append("\n totalStealedTasks: ");
        buf.append(totalStealedTasks);
        buf.append("\n>>> --------------------------");
        buf.append("\n lastTaskExecutionTime: ");
        buf.append(DateTimeFormatter.formatTime(lastTaskExecutionTime));
        buf.append("\n averageTaskExecutionTime: ");
        buf.append(DateTimeFormatter.formatTime(averageTaskExecutionTime));
        buf.append("\n maximumTaskExecutionTime: ");
        buf.append(DateTimeFormatter.formatTime(maximumTaskExecutionTime));
        buf.append("\n lastTaskWaitTime: ");
        buf.append(DateTimeFormatter.formatTime(lastTaskWaitTime));
        buf.append("\n averageTaskWaitTime: ");
        buf.append(DateTimeFormatter.formatTime(averageTaskWaitTime));
        buf.append("\n maximumTaskWaitTime: ");
        buf.append(DateTimeFormatter.formatTime(maximumTaskWaitTime));
        buf.append('\n');

        return buf.toString();
    }

}
