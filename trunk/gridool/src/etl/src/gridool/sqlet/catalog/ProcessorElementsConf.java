/**
 * 
 */
package gridool.sqlet.catalog;

import gridool.GridNode;
import gridool.sqlet.SqletException;
import gridool.sqlet.SqletException.SqletErrorType;
import gridool.sqlet.catalog.PartitioningConf.Partition;
import gridool.util.GridUtils;
import gridool.util.csv.HeaderAwareCsvReader;
import gridool.util.io.FastBufferedInputStream;
import gridool.util.lang.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;

/**
 *
 */
public final class ProcessorElementsConf implements Serializable {
    private static final long serialVersionUID = -150627199290669139L;
    private static final Log LOG = LogFactory.getLog(ProcessorElementsConf.class);

    private final List<ProcessorElement> list;

    public ProcessorElementsConf() {
        this.list = new ArrayList<ProcessorElement>(8);
    }

    public List<ProcessorElement> getProcessorElements() {
        return list;
    }

    public void loadSettings(@Nonnull String uri) throws SqletException {
        if(uri.endsWith(".csv") || uri.endsWith(".CSV")) {
            loadSettingsFromCsv(uri, list);
        } else {
            throw new IllegalArgumentException("Unsupported URI: " + uri);
        }
    }

    private static void loadSettingsFromCsv(String uri, List<ProcessorElement> list)
            throws SqletException {
        final InputStream is;
        try {
            FileSystemManager fsManager = VFS.getManager();
            FileObject fileObj = fsManager.resolveFile(uri);
            FileContent fileContent = fileObj.getContent();
            is = fileContent.getInputStream();
        } catch (FileSystemException e) {
            throw new SqletException(SqletErrorType.configFailed, "failed to load a file: " + uri, e);
        }
        InputStreamReader reader = new InputStreamReader(new FastBufferedInputStream(is));
        HeaderAwareCsvReader csvReader = new HeaderAwareCsvReader(reader, ',', '"');

        final Map<String, Integer> headerMap;
        try {
            headerMap = csvReader.parseHeader();
        } catch (IOException e) {
            throw new SqletException(SqletErrorType.configFailed, "failed to parse a header: "
                    + uri, e);
        }

        final int[] fieldIndexes = toFieldIndexes(headerMap);
        final Map<GridNode, ProcessorElement> masterSlave = new HashMap<GridNode, ProcessorElement>(128);
        while(csvReader.next()) {
            String nodeStr = csvReader.get(fieldIndexes[0]);
            String masterStr = csvReader.get(fieldIndexes[1]);
            String dbUrl = csvReader.get(fieldIndexes[2]);
            String user = csvReader.get(fieldIndexes[3]);
            String password = csvReader.get(fieldIndexes[4]);
            String mapOutput = csvReader.get(fieldIndexes[5]);

            Preconditions.checkNotNull(nodeStr, dbUrl);

            GridNode node = GridUtils.getNode(nodeStr);
            ProcessorElement p = new ProcessorElement(node, dbUrl, user, password, mapOutput);
            if(masterStr == null || masterStr.length() == 0) {
                masterSlave.put(node, p);
                list.add(p);
            } else {
                GridNode master = GridUtils.getNode(masterStr);
                ProcessorElement masterPartition = masterSlave.get(master);
                if(masterPartition == null) {
                    LOG.error("Master partition is not found for slave: " + p);
                } else {
                    masterPartition.addSlave(p);
                }
            }
        }
    }

    private static int[] toFieldIndexes(@Nullable Map<String, Integer> map) {
        if(map == null) {
            return new int[] { 0, 1, 2, 3, 4, 5 };
        } else {
            Integer c0 = map.get("NODE");
            Integer c1 = map.get("MASTER");
            Integer c2 = map.get("DBURL");
            Integer c3 = map.get("USER");
            Integer c4 = map.get("PASSWORD");
            Integer c5 = map.get("MAPOUTPUT");

            Preconditions.checkNotNull(c0, c1, c2, c3, c4, c5);

            final int[] indexes = new int[6];
            indexes[0] = c0.intValue();
            indexes[1] = c1.intValue();
            indexes[2] = c2.intValue();
            indexes[3] = c3.intValue();
            indexes[4] = c4.intValue();
            indexes[5] = c5.intValue();
            return indexes;
        }
    }

    public static final class ProcessorElement implements Serializable {
        private static final long serialVersionUID = -8774183764667248705L;

        @Nonnull
        final GridNode node;
        @Nonnull
        final String dbUrl;
        @Nullable
        final String user;
        @Nullable
        final String password;
        @Nullable
        final String mapOutput;

        ProcessorElement(@Nonnull GridNode node, @Nonnull String dbUrl, @Nullable String user, @Nullable String password, @Nullable String mapOutput) {
            super();
            this.node = node;
            this.dbUrl = dbUrl;
            this.user = user;
            this.password = password;
            this.mapOutput = mapOutput;
        }

        public GridNode getNode() {
            return node;
        }

        public String getDbUrl() {
            return dbUrl;
        }

        public String getUser() {
            return user;
        }

        public String getPassword() {
            return password;
        }

        public String getMapOutput() {
            return mapOutput;
        }

        public List<Partition> getSlaves() {
            return slaves;
        }

    }
}
