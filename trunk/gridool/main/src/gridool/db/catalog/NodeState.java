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
package gridool.db.catalog;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public enum NodeState {

    /* Normal condition */
    normal,
    /* Under maintenance. Should not route tasks */
    underMaintenance,
    /* Suspected as irregular */
    suspected,
    /* Under irregular condition */
    dead;

    private final ReadWriteLock lock;

    private NodeState() {
        this.lock = new ReentrantReadWriteLock();
    }

    public boolean isValid() {
        return this == normal || this == underMaintenance;
    }

    public ReadWriteLock getLock() {
        return lock;
    }

}
