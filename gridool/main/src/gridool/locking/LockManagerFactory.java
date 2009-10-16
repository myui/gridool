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
package gridool.locking;

import gridool.GridConfiguration;

import javax.annotation.Nonnull;

import xbird.config.Settings;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class LockManagerFactory {

    private static final String mgrType = Settings.get("gridool.lockmgr", "vacuum");

    private LockManagerFactory() {}

    public static LockManager createLockManager(@Nonnull GridConfiguration config) {
        if("softkey".equals(mgrType)) {
            return new SoftKeyLockManager();
        } else if("vacuum".equals(mgrType)) {
            return new VacuumingLockManager();
        }
        throw new IllegalStateException("Unexpected LockManager: " + mgrType);
    }
}
