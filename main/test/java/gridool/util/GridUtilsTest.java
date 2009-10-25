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
package gridool.util;

import junit.framework.TestCase;
import xbird.util.string.StringUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public class GridUtilsTest extends TestCase {

    public GridUtilsTest(String name) {
        super(name);
    }

    /**
     * Test method for {@link gridool.util.GridUtils#generateLockKey(java.lang.String, byte[])}.
     */
    public void testGenerateLockKey() {
        String idxName = "idx1";
        String key1Str = "key1";
        byte[] key1 = StringUtils.getBytes(key1Str);

        byte[] generated = GridUtils.generateLockKey(idxName, key1);
        String generatedStr = StringUtils.toString(generated);
        System.out.println(generatedStr);
        assertEquals(idxName + ' ' + key1Str, generatedStr);
    }

}
