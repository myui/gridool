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
package gridool.util.annotation;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class BasicResourceInjector implements ResourceInjector {

    @Nonnull
    private Object injectResource;

    public BasicResourceInjector(@CheckForNull Object injectResource) {
        if(injectResource == null) {
            throw new IllegalArgumentException();
        }
        this.injectResource = injectResource;
    }

    public void setResource(@CheckForNull Object injectResource) {
        if(injectResource == null) {
            throw new IllegalArgumentException();
        }
        this.injectResource = injectResource;
    }

    public void inject(Field f, Object target, Annotation ann) throws IllegalArgumentException,
            IllegalAccessException {
        f.setAccessible(true);
        f.set(target, injectResource);
    }

}
