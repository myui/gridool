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
package gridool.deployment;

import gridool.GridException;
import gridool.GridNode;
import gridool.construct.GridTaskAdapter;

import java.io.IOException;

import javax.annotation.Nonnull;

import org.apache.commons.logging.LogFactory;

import xbird.util.lang.ClassUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridGetClassTask extends GridTaskAdapter {
    private static final long serialVersionUID = -947147707833210277L;

    private final String className;

    public GridGetClassTask(@Nonnull String jobId, @Nonnull GridNode senderNode, @Nonnull String className) {
        super(jobId, senderNode, false);
        this.className = className;
    }

    @Override
    public boolean injectResources() {
        return false;
    }

    public ClassData execute() throws GridException {
        final Class<?> clazz;
        try {
            clazz = Class.forName(className, true, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException e) {
            String errmsg = "Class not found: " + className;
            LogFactory.getLog(getClass()).error(errmsg, e);
            throw new GridException(errmsg, e);
        }
        final byte[] b;
        try {
            b = ClassUtils.getClassAsBytes(clazz);
        } catch (IOException e) {
            String errmsg = "Failed serializing a class: " + clazz.getName();
            LogFactory.getLog(getClass()).error(errmsg, e);
            throw new GridException(errmsg, e);
        }
        final long timestamp = ClassUtils.getLastModified(clazz);

        final ClassData innerClassData = new ClassData(b, timestamp);
        ClassData classData = innerClassData;
        Class<?> enclosingClass = clazz.getEnclosingClass();
        while(enclosingClass != null) {
            classData = addEnclosingClass(classData, enclosingClass);
            enclosingClass = enclosingClass.getEnclosingClass();
        }
        return innerClassData;
    }

    /**
     * @return outer class
     */
    private static ClassData addEnclosingClass(ClassData classData, Class<?> enclosingClass)
            throws GridException {
        final String clsName = enclosingClass.getName();
        final byte[] b;
        try {
            b = ClassUtils.getClassAsBytes(enclosingClass);
        } catch (IOException e) {
            String msg = "Failed serializing a class: " + enclosingClass.getName();
            LogFactory.getLog(GridGetClassTask.class).error(msg, e);
            throw new GridException(msg, e);
        }
        final long timestamp = ClassUtils.getLastModified(enclosingClass);
        ClassData outerClazz = new ClassData(clsName, b, timestamp);
        classData.setEnclosingClass(outerClazz);
        return outerClazz;
    }

}
