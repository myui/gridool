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
package gridool.tools;

import gridool.Grid;
import gridool.GridClient;
import gridool.GridException;
import gridool.communication.payload.GridNodeInfo;
import gridool.communication.transport.CommunicationServiceBase;
import gridool.directory.job.DirectoryAddGridNodeJob;
import gridool.directory.ops.AddGridNodeOperation;

import java.io.File;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import xbird.config.Settings;
import xbird.storage.DbCollection;
import xbird.util.lang.Primitives;
import xbird.util.net.NetUtils;
import xbird.util.string.StringUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class MetaDataDistributor {

    @Option(name = "-col", usage = "Collection to distributed on grid", metaVar = "Filespec")
    private String colpath = null;

    @Option(name = "-ep", usage = "Grid remote endpoint")
    private String remoteEndpoint = null;

    @Option(name = "-port", usage = "Grid transport port")
    private int port = Primitives.parseInt(Settings.get("gridool.transport.port"), CommunicationServiceBase.DEFAULT_PORT);

    public MetaDataDistributor() {}

    public void distributedMetaData(Grid grid) {
        final GridNodeInfo localNode = new GridNodeInfo(NetUtils.getLocalHost(), port, false);
        final byte[][] paths = listRelativePaths(colpath);
        try {
            grid.execute(DirectoryAddGridNodeJob.class, new AddGridNodeOperation(paths, localNode));
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }

    private static byte[][] listRelativePaths(final String colPath) {
        DbCollection col = colPath == null ? DbCollection.getRootCollection()
                : DbCollection.getCollection(colPath);
        List<File> files = col.listDocumentFiles(true);
        final List<byte[]> paths = new ArrayList<byte[]>(files.size());

        final boolean replaceRequired = File.separatorChar != '/';
        String rootPath = DbCollection.getRootCollection().getDirectory().getAbsolutePath();
        final int rootLength = rootPath.length();
        for(File f : files) {
            String absPath = f.getAbsolutePath();
            String relPath = absPath.substring(rootLength);
            if(replaceRequired) {
                relPath = relPath.replace(File.separatorChar, '/');
            }
            int lastDotPos = relPath.lastIndexOf('.');
            String relDocPath = relPath.substring(0, lastDotPos);
            paths.add(StringUtils.getBytes(relDocPath));
        }

        int size = paths.size();
        final byte[][] ary = new byte[size][];
        paths.toArray(ary);
        return ary;
    }

    // ----------------------------------------------------

    public static void main(String[] args) throws GridException {
        new MetaDataDistributor().doMain(args);
    }

    private void doMain(String[] args) throws GridException {
        prepArgs(args);
        Grid grid = new GridClient(remoteEndpoint);
        distributedMetaData(grid);
    }

    private void prepArgs(String[] args) {
        final CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            e.printStackTrace();
            return;
        }
    }

}
