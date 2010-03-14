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
package gridool.tools;

import gridool.GridException;
import gridool.GridService;

import java.lang.management.ThreadInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.config.Settings;
import xbird.util.concurrent.ThreadDeadlockDetector;
import xbird.util.primitive.Primitives;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class GridDeadlockDetectionService implements GridService {

    private static final long detectionPeriod;
    static {
        detectionPeriod = Primitives.parseLong(Settings.get("gridool.deadlock_detection_period"), 180000L);
    }
    private final ThreadDeadlockDetector deadlockDetector;

    public GridDeadlockDetectionService() {
        if(detectionPeriod > 0) {
            deadlockDetector = new ThreadDeadlockDetector(detectionPeriod);
            deadlockDetector.addListener(new LogDeadlockReporter());
        } else {
            deadlockDetector = null;
        }
    }

    @Override
    public String getServiceName() {
        return GridDeadlockDetectionService.class.getName();
    }

    @Override
    public boolean isDaemon() {
        return true;
    }

    @Override
    public void start() throws GridException {
        if(deadlockDetector != null) {
            deadlockDetector.start();
        }
    }

    @Override
    public void stop() throws GridException {
        if(deadlockDetector != null) {
            deadlockDetector.stop();
        }
    }

    private static class LogDeadlockReporter implements ThreadDeadlockDetector.Listener {
        static final Log LOG = LogFactory.getLog(LogDeadlockReporter.class);

        LogDeadlockReporter() {}

        @Override
        public void deadlockDetected(ThreadInfo[] deadlockedThreads) {
            final StringBuilder buf = new StringBuilder(512);
            buf.append("Deadlocked Threads:\n");
            buf.append("-------------------\n");
            for(ThreadInfo ti : deadlockedThreads) {
                buf.append(ti.toString());
                buf.append('\n');
            }
            if(LOG.isWarnEnabled()) {
                LOG.warn(buf.toString());
            }
        }

    }

}
