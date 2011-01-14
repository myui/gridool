import gridool.GridClient;
import gridool.GridJobDesc;

import java.rmi.RemoteException;

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

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class HelloWorldSample1 {

    public HelloWorldSample1() {}

    public static void main(String[] args) throws RemoteException {
        GridClient grid = new GridClient();
        String result = grid.execute(new GridJobDesc(Sample1.class), "Hello World!");
        System.out.println(result);
        //Integer count = grid.execute(Sample1.class, "Hello World!");
        //System.out.println("Total word count: " + count);
    }

}
