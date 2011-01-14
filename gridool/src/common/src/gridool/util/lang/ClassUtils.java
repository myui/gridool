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
package gridool.util.lang;

import gridool.util.io.FileUtils;
import gridool.util.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class ClassUtils {

    private ClassUtils() {}

    @Nonnull
    public static String getSimpleClassName(@Nullable Object obj) {
        if(obj == null) {
            return "null";
        }
        return getSimpleClassName(obj.getClass());
    }

    @Nonnull
    public static String getSimpleClassName(@Nonnull Class<?> cls) {
        String className = cls.getName();
        int idx = className.lastIndexOf(".");
        return idx == -1 ? className : className.substring(idx + 1);
    }

    @Nonnull
    public static String getRelativeClassFilePath(@Nonnull String className) {
        return className.replace('.', '/') + ".class";
    }

    @SuppressWarnings("deprecation")
    @Nonnull
    public static File getClassFile(@Nonnull Class<?> clazz) {
        String className = clazz.getName();
        String path = getRelativeClassFilePath(className);
        URL url = clazz.getResource('/' + path);
        String absolutePath = url.getFile();
        String decoded = URLDecoder.decode(absolutePath);
        return new File(decoded);
    }

    public static File getClassFile(@Nonnull Class<?> clazz, ClassLoader cl) {
        String className = clazz.getName();
        String path = getRelativeClassFilePath(className);
        URL url = cl.getResource('/' + path);
        String absolutePath = url.getFile();
        final String decoded;
        try {
            decoded = URLDecoder.decode(absolutePath, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
        return new File(decoded);
    }

    public static byte[] getClassAsBytes(@Nonnull Class<?> clazz) throws IOException {
        InputStream is = getClassAsStream(clazz);
        return IOUtils.getBytes(is);
    }

    public static InputStream getClassAsStream(@Nonnull Class<?> clazz) throws IOException {
        String className = clazz.getName();
        String path = getRelativeClassFilePath(className);
        URL url = clazz.getResource('/' + path);
        return url.openStream();
    }

    public static File getJarFile(@Nonnull Class<?> clazz) {
        File file = getClassFile(clazz);
        String path = file.getPath();
        final int idx = path.lastIndexOf('!');
        if(idx == -1) {
            return null;
        }
        String jarFilePath = path.substring(0, idx);
        if(jarFilePath.startsWith("file:\\")) {// workaround for windows
            jarFilePath = jarFilePath.substring(6);
        } else if(jarFilePath.startsWith("file:/")) {
            jarFilePath = jarFilePath.substring(5);
        }
        file = new File(jarFilePath);
        return file;
    }

    public static URL getJarURL(@Nonnull Class<?> clazz) {
        File file = getJarFile(clazz);
        if(file == null) {
            return null;
        }
        if(!file.exists()) {
            return null;
        }
        URI uri = file.toURI();
        try {
            return uri.toURL();
        } catch (MalformedURLException e) {
            return null;
        }
    }

    public static String getJarFilePath(@Nonnull Class<?> clazz) {
        File file = getJarFile(clazz);
        if(file == null) {
            return null;
        }
        if(!file.exists()) {
            return null;
        }
        return file.getAbsolutePath();
    }

    public static long getLastModified(@Nonnull Class<?> clazz) {
        File file = getClassFile(clazz);
        String path = file.getPath();
        final int idx = path.lastIndexOf('!');
        if(idx != -1) {
            String jarFilePath = path.substring(0, idx);
            if(jarFilePath.startsWith("file:\\")) {// workaround for windows
                jarFilePath = jarFilePath.substring(6);
            } else if(jarFilePath.startsWith("file:/")) {
                jarFilePath = jarFilePath.substring(5);
            }
            file = new File(jarFilePath);
        }
        if(file.exists()) {
            return file.lastModified();
        }
        return -1L;
    }

    @Deprecated
    public static Map<String, File> getInnerClassFiles(@Nonnull Class<?> clazz, boolean includeSelf) {
        final Map<String, File> m = new LinkedHashMap<String, File>(8);
        final File file = getClassFile(clazz);
        if(includeSelf) {
            String clsName = clazz.getName();
            m.put(clsName, file);
        }
        File directory = file.getParentFile();
        String simpleName = clazz.getSimpleName();
        final List<File> list = FileUtils.listFiles(directory, new String[] { simpleName }, new String[] { ".class" }, false);
        for(File f : list) {
            String fname = f.getName();
            int idx = fname.lastIndexOf('.');
            String clsName = fname.substring(0, idx);
            m.put(clsName, f);
        }
        return m;
    }
}
