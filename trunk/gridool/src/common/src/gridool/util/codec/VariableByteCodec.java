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
package gridool.util.codec;

import gridool.util.io.FastByteArrayInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


/**
 * First 1 bit of each byte is frag to judge whether more lookahead is required or not. 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 * @link http://www.cs.otago.ac.nz/cosc463/2005/compress.htm
 */
public final class VariableByteCodec {

    public VariableByteCodec() {}

    public static int requiredBytes(long val) {
        int i = 1;
        while(val > 0x7FL) {
            val >>= 7;
            i++;
        }
        return i;
    }

    public static int requiredBytes(int val) {
        int i = 1;
        while(val > 0x7F) {
            val >>= 7;
            i++;
        }
        return i;
    }

    public static byte[] encodeLong(long val) {
        if(val < 0) {
            throw new IllegalArgumentException("Illegal value: " + val);
        }
        final byte[] buf = new byte[9];
        int i = 0;
        while(val > 0x7FL) {
            buf[i++] = (byte) ((val & 0x7F) | 0x80);
            val >>= 7;
        }
        buf[i++] = (byte) val;
        final byte[] rbuf = new byte[i];
        System.arraycopy(buf, 0, rbuf, 0, i);
        return rbuf;
    }

    public static void encodeLong(long val, byte[] out, int offset) {
        if(val < 0) {
            throw new IllegalArgumentException("Illegal value: " + val);
        }
        while(val > 0x7FL) {
            out[offset++] = (byte) ((val & 0x7F) | 0x80);
            val >>= 7;
        }
        out[offset++] = (byte) val;
    }

    public static void encodeLong(long val, final OutputStream os) throws IOException {
        if(val < 0) {
            throw new IllegalArgumentException("Illegal value: " + val);
        }
        while(val > 0x7FL) {
            final byte b = (byte) ((val & 0x7F) | 0x80);
            os.write(b);
            val >>= 7;
        }
        final byte b = (byte) val;
        os.write(b);
    }

    public static long decodeLong(final byte[] val) {
        return decodeLong(val, 0);
    }

    public static long decodeLong(final byte[] val, final int from) {
        long x = 0;
        long b = 0;
        int shift = 0;

        final int vlen = val.length;
        for(int i = from; i < vlen; i++) {
            b = val[i];
            final long more = b & 0x80L;
            x |= (b & 0x7FL) << shift;
            if(more != 0x80L) {
                break;
            }
            shift += 7;
        }
        return x;
    }

    public static long decodeLong(final InputStream is) throws IOException {
        long x = 0;
        long b = 0;
        int shift = 0;

        while(true) {
            b = is.read();
            x |= (b & 0x7FL) << shift;
            final long more = b & 0x80L;
            if(more != 0x80L) {
                break;
            }
            shift += 7;
        }
        return x;
    }

    public static void encodeInt(int val, final OutputStream os) throws IOException {
        if(val < 0) {
            throw new IllegalArgumentException("Illegal value: " + val);
        }
        while(val > 0x7F) {
            final byte b = (byte) ((val & 0x7F) | 0x80);
            os.write(b);
            val >>= 7;
        }
        final byte b = (byte) val;
        os.write(b);
    }

    public static int decodeInt(final InputStream is) throws IOException {
        int x = 0;
        int b = 0;
        int shift = 0;

        while(true) {
            b = is.read();
            x |= (b & 0x7F) << shift;
            if((b & 0x80) != 0x80) {
                break;
            }
            shift += 7;
        }
        return x;
    }

    public static int decodeInt(final InputStream is, int b) throws IOException {
        int x = 0;
        int shift = 0;

        while(true) {
            x |= (b & 0x7F) << shift;
            if((b & 0x80) != 0x80) {
                break;
            }
            shift += 7;
            b = is.read();
        }
        return x;
    }

    public static int decodeInt(final FastByteArrayInputStream is) {
        int x = 0;
        int b = 0;
        int shift = 0;

        while(true) {
            b = is.read();
            x |= (b & 0x7F) << shift;
            if((b & 0x80) != 0x80) {
                break;
            }
            shift += 7;
        }
        return x;
    }

    public static int decodeInt(final byte[] val) {
        return decodeInt(val, 0);
    }

    public static int decodeInt(final byte[] val, final int from) {
        int x = 0;
        int b = 0;
        int shift = 0;

        final int vlen = val.length;
        for(int i = from; i < vlen; i++) {
            b = val[i];
            x |= (b & 0x7F) << shift;
            if((b & 0x80) != 0x80) {
                break;
            }
            shift += 7;
        }
        return x;
    }
}
