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

import javax.annotation.Nonnull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public enum NodeState {

    /* Normal condition */
    normal(0),
    /* Under maintenance. Should not route tasks */
    underMaintenance(1),
    /* Suspected as irregular */
    suspected(-2),
    /* Under irregular condition */
    dead(-1);

    private final int stateNumber;
    private final ReadWriteLock lock;

    private NodeState(int stateNumber) {
        this.stateNumber = stateNumber;
        this.lock = new ReentrantReadWriteLock();
    }

    public int getStateNumber() {
        return stateNumber;
    }

    public boolean isValid() {
        return this == normal || this == underMaintenance;
    }

    @Nonnull
    public ReadWriteLock getLock() {//TODO REVIEWME
        return lock;
    }

    public static NodeState resolve(final int stateNo) {
        switch(stateNo) {
            case 0:
                return normal;
            case 1:
                return underMaintenance;
            case -2:
                return suspected;
            case -1:
                return dead;
            default:
                throw new IllegalArgumentException("Unexpected stateNo: " + stateNo);
        }
    }

}
