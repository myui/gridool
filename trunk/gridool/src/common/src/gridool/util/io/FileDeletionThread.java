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
package gridool.util.io;

import gridool.Settings;
import gridool.util.primitive.Primitives;

import java.io.File;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;

/**
 * 
 * <DIV lang="en">
 * The following options are required to enable setting priority.
 * -XX:+UseThreadPriorities
 * -XX:ThreadPriorityPolicy=42
 * </DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 * @link https://issues.apache.org/jira/browse/CASSANDRA-1181
 */
public final class FileDeletionThread extends Thread {

    private static final int DEFAULT_PRIORITY;
    static {
        DEFAULT_PRIORITY = Primitives.parseInt(Settings.get("gridool.io.file_deletion_thread_priority"), Thread.NORM_PRIORITY - 2);
    }

    @Nonnull
    private final File file;
    @Nullable
    private final Log logger;

    public FileDeletionThread(@Nonnull File file) {
        this(file, null);
    }

    public FileDeletionThread(@Nonnull File file, @Nullable Log logger) {
        this(file, logger, DEFAULT_PRIORITY);
    }

    public FileDeletionThread(@Nonnull File file, @Nullable Log logger, int priority) {
        super("FileDeletionThread");
        if(file == null) {
            throw new IllegalArgumentException();
        }
        this.file = file;
        this.logger = logger;
        setPriority(priority);
    }

    @Override
    public void run() {
        if(file.exists()) {
            if(file.delete()) {
                if(logger != null && logger.isDebugEnabled()) {
                    logger.debug("Deleted a file: " + file.getAbsolutePath());
                }
            } else {
                if(logger != null) {
                    logger.warn("Could not delete a file: " + file.getAbsolutePath());
                }
            }
        }
    }

}
