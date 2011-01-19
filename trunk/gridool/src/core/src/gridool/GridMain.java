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

import gridool.util.remoting.InternalException;

import java.rmi.RemoteException;

import org.apache.commons.logging.LogFactory;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridMain {

    private GridMain() {}

    public static void main(String[] args) {
        final Thread parent = Thread.currentThread();
        final GridServer grid = new GridServer();
        try {
            grid.start();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    parent.interrupt();
                }
            });
        } catch (InternalException ie) {
            shutdown(grid, ie);
        } catch (Throwable e) {
            shutdown(grid, e);
        }
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            ;
        } finally {
            try {
                grid.shutdown(true);
            } catch (RemoteException re) {
                ;
            }
        }
    }

    private static void shutdown(final GridServer grid, final Throwable e) {
        String errmsg = "Failed to start grid: " + e.getMessage();
        LogFactory.getLog(GridMain.class).error(errmsg, e);
        System.err.println(errmsg);
        try {
            grid.shutdown(true);
        } catch (RemoteException re) {
            ;
        }
    }

}
