/*
 * @(#)$Id$$
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
package gridool.util.net;

import java.io.IOException;
import java.net.*;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class TimeoutServerSocket extends ServerSocket {

    private final int timeout;

    public TimeoutServerSocket(int timeout, int port, int backlog, InetAddress bindAddr)
            throws IOException {
        super(port, backlog, bindAddr);
        this.timeout = timeout;
    }

    @Override
    public Socket accept() throws IOException {
        Socket s = super.accept();
        TimeoutSocket ts = new TimeoutSocket(s, timeout);
        return ts;
    }
}
