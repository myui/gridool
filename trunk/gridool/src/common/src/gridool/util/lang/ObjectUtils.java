/*
 * @(#)$Id: ObjectUtils.java 3619 2008-03-26 07:23:03Z yui $
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

import gridool.util.io.CustomObjectInputStream;
import gridool.util.io.FastByteArrayInputStream;
import gridool.util.io.FastMultiByteArrayOutputStream;
import gridool.util.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class ObjectUtils {

    private ObjectUtils() {}

    /**
     * Returns String expression of the given Object.
     * 
     * this method returns a string equal to the value of:<br/>
     *   getClass().getSimpleName() + '@' + Integer.toHexString(hashCode())
     */
    public static String identityToString(final Object obj) {
        if(obj == null) {
            return null;
        }
        return obj.getClass().getSimpleName() + '@'
                + Integer.toHexString(System.identityHashCode(obj));
    }

    public static <T> T instantiate(final String clazz) {
        try {
            return (T) instantiate(Class.forName(clazz));
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Class not found.. " + clazz);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T instantiateSafely(final String className) {
        try {
            Class clazz = Class.forName(className);
            return (T) clazz.newInstance();
        } catch (ClassNotFoundException cne) {
            return null;
        } catch (InstantiationException ie) {
            return null;
        } catch (IllegalAccessException iae) {
            return null;
        } catch (Throwable e) {
            return null;
        }
    }

    public static <T> T instantiate(final String clazz, final Class[] parameterTypes, final Object... args) {
        try {
            return (T) instantiate(Class.forName(clazz), parameterTypes, args);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Class not found.. " + clazz);
        }
    }

    public static <T> T instantiate(final Class clazz) {
        try {
            return (T) clazz.newInstance();
        } catch (Exception e) {
            throw new IllegalStateException("Object could not instantiate for class.. " + clazz, e);
        }
    }

    public static <T> T instantiate(final String className, final ClassLoader cl) {
        try {
            Class c = ClassResolver.loadClass(className, cl);
            return (T) c.newInstance();
        } catch (Exception e) {
            throw new IllegalStateException("Object could not instantiate for class.. " + className, e);
        }
    }

    public static <T> T instantiate(final Class clazz, final Class[] parameterTypes, final Object... args) {
        try {
            final Constructor<T> constructor = clazz.getConstructor(parameterTypes);
            return constructor.newInstance(args);
        } catch (Exception e) {
            throw new IllegalStateException("Object could not instantiate for `" + clazz + "("
                    + parameterTypes + ")` with arguments `" + args + '`', e);
        }
    }

    public static boolean hasDefaultConstructor(final String clazz) {
        try {
            return hasDefaultConstructor(Class.forName(clazz));
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Class not found.. " + clazz);
        }
    }

    public static boolean hasDefaultConstructor(final Class clazz) {
        final Constructor[] constructors = clazz.getConstructors();
        for(Constructor c : constructors) {
            if(c.getParameterTypes().length == 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * @link http://javatechniques.com/public/java/docs/basics/faster-deep-copy.html
     */
    public static <T> T deepCopy(final Object orig) {
        final Object obj;
        try {
            // write the object
            final FastMultiByteArrayOutputStream fbos = new FastMultiByteArrayOutputStream();
            final ObjectOutputStream out = new ObjectOutputStream(fbos);
            out.writeObject(orig);
            out.flush();
            out.close();
            // read an object
            final ObjectInputStream in = new ObjectInputStream(fbos.getInputStream());
            obj = in.readObject();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalStateException(cnfe);
        }
        return (T) obj;
    }

    public static byte[] toBytes(final Object obj) {
        final FastMultiByteArrayOutputStream bos = new FastMultiByteArrayOutputStream();
        toStream(obj, bos);
        return bos.toByteArray_clear();
    }

    public static byte[] toBytes(final Object obj, final int estimatedLen) {
        final FastMultiByteArrayOutputStream bos = new FastMultiByteArrayOutputStream(estimatedLen * 2);
        toStream(obj, bos);
        return bos.toByteArray_clear();
    }

    public static byte[] toGzipCompressedBytes(final Object obj) {
        final FastMultiByteArrayOutputStream bos = new FastMultiByteArrayOutputStream();
        final GZIPOutputStream gos;
        try {
            gos = new GZIPOutputStream(bos);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        toStream(obj, gos);
        return bos.toByteArray_clear();
    }

    public static void toStream(final Object obj, final OutputStream out) {
        try {
            final ObjectOutputStream oos = new ObjectOutputStream(out);
            oos.writeObject(obj);
            oos.flush();
            oos.close();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static void toStreamVerbose(final Object obj, final OutputStream out) throws IOException {
        try {
            final ObjectOutputStream oos = new ObjectOutputStream(out);
            oos.writeObject(obj);
            oos.flush();
            oos.close();
        } catch (IOException ioe) {
            throw ioe;
        } catch (Throwable e) {
            throw new IOException(PrintUtils.prettyPrintStackTrace(e));
        }
    }

    public static <T> T readObjectQuietly(final byte[] obj) {
        return ObjectUtils.<T> readObjectQuietly(new FastByteArrayInputStream(obj));
    }

    public static <T> T readObjectQuietly(final byte[] obj, final ClassLoader cl) {
        return ObjectUtils.<T> readObjectQuietly(new FastByteArrayInputStream(obj), cl);
    }

    public static <T> T readGzipCompressedObjectQuietly(final byte[] obj) {
        final FastByteArrayInputStream bis = new FastByteArrayInputStream(obj);
        final GZIPInputStream gis;
        try {
            gis = new GZIPInputStream(bis);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return ObjectUtils.<T> readObjectQuietly(gis);
    }

    public static <T> T readObject(final byte[] obj) throws IOException, ClassNotFoundException {
        return ObjectUtils.<T> readObject(new FastByteArrayInputStream(obj));
    }

    public static <T> T readGzipCompressedObject(final byte[] obj) throws IOException,
            ClassNotFoundException {
        FastByteArrayInputStream bis = new FastByteArrayInputStream(obj);
        GZIPInputStream gis = new GZIPInputStream(bis);
        return ObjectUtils.<T> readObject(gis);
    }

    public static <T> T readObject(final byte[] obj, final ClassLoader cl) throws IOException,
            ClassNotFoundException {
        return ObjectUtils.<T> readObject(new FastByteArrayInputStream(obj), cl);
    }

    public static <T> T readObjectQuietly(final InputStream is) {
        try {
            final ObjectInputStream ois = new ObjectInputStream(is);
            return (T) ois.readObject();
        } catch (IOException ioe) {
            IOUtils.closeQuietly(is);
            throw new IllegalStateException(ioe);
        } catch (ClassNotFoundException ce) {
            IOUtils.closeQuietly(is);
            throw new IllegalStateException(ce);
        }
    }

    public static <T> T readObjectQuietly(final InputStream is, final ClassLoader cl) {
        try {
            final ObjectInputStream ois = new CustomObjectInputStream(is, cl);
            return (T) ois.readObject();
        } catch (IOException ioe) {
            IOUtils.closeQuietly(is);
            throw new IllegalStateException(ioe);
        } catch (ClassNotFoundException ce) {
            IOUtils.closeQuietly(is);
            throw new IllegalStateException(ce);
        }
    }

    public static <T> T readObject(final InputStream is) throws IOException, ClassNotFoundException {
        final ObjectInputStream ois = new ObjectInputStream(is);
        return (T) ois.readObject();
    }

    public static <T> T readObject(final InputStream is, final ClassLoader cl) throws IOException,
            ClassNotFoundException {
        final ObjectInputStream ois = new CustomObjectInputStream(is, cl);
        return (T) ois.readObject();
    }

    public static String readString(final ObjectInput in) throws IOException {
        final int len = in.readInt();
        final char[] chrs = new char[len];
        for(int i = 0; i < len; i++) {
            chrs[i] = in.readChar();
        }
        return new String(chrs);
    }

    public static void writeString(final ObjectOutput out, final String s) throws IOException {
        final int len = s.length();
        out.writeInt(len);
        for(int i = 0; i < len; i++) {
            int v = s.charAt(i);
            out.write((v >>> 8) & 0xFF);
            out.write((v >>> 0) & 0xFF);
        }
    }

}