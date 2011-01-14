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
package gridool.example;

import java.util.concurrent.ExecutionException;

import gridool.GridException;
import gridool.GridFactory;
import gridool.GridJobFuture;
import gridool.GridKernel;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class HelloWorldExample {

    public HelloWorldExample() {}

    private static void doWork(GridKernel grid) throws InterruptedException, ExecutionException {
        GridJobFuture<Integer> future = grid.execute(GridHelloWorldJob.class, "Hello World");
        System.out.println("Total word count: " + future.get());
    }
    
    public static void main(String[] args) throws GridException, InterruptedException, ExecutionException {
        final GridKernel grid = GridFactory.makeGrid();        
        grid.start();
        try {
            doWork(grid);
        } finally {
            grid.stop(true);
        }
    }

}
