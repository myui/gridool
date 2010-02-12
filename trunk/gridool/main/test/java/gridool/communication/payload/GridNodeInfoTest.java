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
package gridool.communication.payload;

import junit.framework.TestCase;
import xbird.util.net.NetUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class GridNodeInfoTest extends TestCase {

    public void testSerialize1() {
        int port = 3333;
        boolean isSuper = true;
        GridNodeInfo node = new GridNodeInfo(port, isSuper);
        byte[] b = node.toBytes();
        GridNodeInfo node2 = GridNodeInfo.fromBytes(b);
        assertEquals(NetUtils.getLocalHost(), node2.getPhysicalAdress());
        assertEquals(port, node2.getPort());
        assertEquals(isSuper, node2.isSuperNode());
    }

    public void testSerialize2() {
        int port = 3333;
        boolean isSuper = true;
        GridNodeInfo node = new GridNodeInfo(port, isSuper);
        byte[] b = node.toBytes();
        GridNodeInfo node2 = GridNodeInfo.fromBytes(b);
        assertEquals(NetUtils.getLocalHost(), node2.getPhysicalAdress());
        assertEquals(port, node2.getPort());
        assertEquals(isSuper, node2.isSuperNode());
        assertEquals(node.getKey(), node2.getKey());
    }
}
