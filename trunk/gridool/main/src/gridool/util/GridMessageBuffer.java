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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gridool.communication.GridCommunicationMessage;
import xbird.util.io.FastByteArrayInputStream;
import xbird.util.io.FastByteArrayOutputStream;
import xbird.util.lang.ObjectUtils;
import xbird.util.primitives.Primitives;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
@NotThreadSafe
public final class GridMessageBuffer {
    private static final Log LOG = LogFactory.getLog(GridMessageBuffer.class);

    private int msgSize = -1;
    private final FastByteArrayOutputStream msgBuf = new FastByteArrayOutputStream(8);

    public GridMessageBuffer() {}

    public GridCommunicationMessage toMessage() {
        final InputStream is = new FastByteArrayInputStream(msgBuf.getInternalArray(), 0, msgBuf.size());
        try {
            return ObjectUtils.readObject(is);
        } catch (IOException ioe) {
            LOG.error(ioe.getMessage());
            throw new IllegalStateException(ioe);
        } catch (ClassNotFoundException cnfe) {
            LOG.error(cnfe.getMessage());
            throw new IllegalStateException(cnfe);
        } catch (Throwable te) {
            LOG.fatal(te.getMessage(), te);
            throw new IllegalStateException(te);
        }
    }

    public boolean isFilled() {
        return msgSize > 0 && msgBuf.size() == msgSize;
    }

    public void read(final ByteBuffer buf) {
        if(msgSize < 0) {// read a message header (and data) here
            final int remaining = buf.remaining();
            if(remaining <= 0) {
                return;
            }
            final int missing = 4 - msgBuf.size();
            if(remaining < missing) {
                msgBuf.write(buf, remaining);
                return;
            } else {
                msgBuf.write(buf, missing);
                assert (msgBuf.size() <= 4) : msgBuf.size();
                if(msgBuf.size() == 4) {
                    final int size = Primitives.getInt(msgBuf.getInternalArray(), 0);
                    assert (size > 0) : size;
                    if(LOG.isDebugEnabled()) {
                        LOG.debug("Receiving message [id=" + this.hashCode() + ", size=" + size
                                + ']');
                    }
                    this.msgSize = size;
                    msgBuf.reset(size);
                }
            }
        }
        final int remaining = buf.remaining();
        if(remaining > 0) {
            final int missing = msgSize - msgBuf.size();
            if(missing > 0) {// read up to message size
                msgBuf.write(buf, remaining > missing ? missing : remaining);
                if(LOG.isTraceEnabled()) {
                    LOG.trace("Receiving message [id=" + this.hashCode() + ", size=" + msgSize
                            + ", msgBuf.size()=" + msgBuf.size() + "], filled=" + isFilled());
                }
            }
        }
    }

    public void reset() {
        msgBuf.reset();
        msgSize = -1;
    }

    public void clear() {
        msgBuf.clear();
        msgSize = -1;
    }

}
