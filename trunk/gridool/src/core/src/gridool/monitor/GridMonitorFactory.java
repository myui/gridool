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
package gridool.monitor;

import gridool.GridExecutionListener;
import gridool.GridResourceRegistry;
import gridool.Settings;
import gridool.util.lang.ObjectUtils;

import javax.annotation.Nonnull;

import org.apache.commons.logging.LogFactory;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridMonitorFactory {

    private GridMonitorFactory() {}

    public static GridExecutionMonitor createExecutionMonitor(GridResourceRegistry registry) {
        GridExecutionMonitor monitor = new GridExecutionMonitor(registry);

        String additional = Settings.get("grid.monitor.exec_listener");
        if(additional != null) {
            setupAdditionalListeners(monitor, additional);
        }

        registry.setExecutionMonitor(monitor);
        return monitor;
    }

    private static void setupAdditionalListeners(@Nonnull GridExecutionMonitor monitor, @Nonnull String additional) {
        final String[] listeners = additional.split(",");
        for(String listenerClazz : listeners) {
            Object obj = ObjectUtils.instantiateSafely(listenerClazz.trim());
            if(obj != null && obj instanceof GridExecutionListener) {
                GridExecutionListener listener = (GridExecutionListener) obj;
                monitor.addListener(listener);
            } else {
                LogFactory.getLog(GridMonitorFactory.class).warn("Specified GridExecutionListener not found: "
                        + listenerClazz);
            }
        }
    }
}
