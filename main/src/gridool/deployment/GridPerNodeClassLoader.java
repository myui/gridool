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
package gridool.deployment;

import gridool.GridNode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class GridPerNodeClassLoader extends ClassLoader {
    private static final Log LOG = LogFactory.getLog(GridPerNodeClassLoader.class);

    @Nonnull
    private final GridNode node;
    @Nonnull
    private final Map<String, Long> networkLoadedClazzWithTimestamp;

    public GridPerNodeClassLoader(@Nonnull GridNode node) {
        this(node, Thread.currentThread().getContextClassLoader());
    }

    public GridPerNodeClassLoader(@Nonnull GridNode node, @Nullable ClassLoader parent) {
        super(parent);
        this.node = node;
        this.networkLoadedClazzWithTimestamp = new ConcurrentHashMap<String, Long>();
    }
    
    @Nonnull
    public GridNode getNode() {
        return node;
    }

    public Long getTimestamp(String clsName) {
        return networkLoadedClazzWithTimestamp.get(clsName);
    }

    Class<?> defineClassIfNeeded(String clsName, byte[] b, long timestamp) {
        final Class<?> loaded = findLoadedClass(clsName);
        if(loaded != null) {
            final Long prevTimestamp = networkLoadedClazzWithTimestamp.get(clsName);
            if(prevTimestamp == null) {// the given class is already loaded locally
                return loaded;
            }
            if(prevTimestamp.longValue() >= timestamp) {// previously loaded class is older than the given class
                return loaded;
            }
            return null;
        }
        final Class<?> defined = defineClass(clsName, b, 0, b.length);
        networkLoadedClazzWithTimestamp.put(clsName, timestamp);
        if(LOG.isInfoEnabled()) {
            LOG.info("A class '" + clsName + "' with timestamp '" + timestamp + "' is defined");
        }
        return defined;
    }
}
