/*
 * @(#)$Id$
 *
 * Copyright 2009-2010 Makoto YUI
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
package gridool.sqlet.catalog;

import gridool.GridNode;
import gridool.sqlet.SqletException;
import gridool.sqlet.SqletException.SqletErrorType;
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
import java.util.LinkedList;
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
 * <DIV lang="en"></DIV> <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class PartitioningConf implements Serializable {
	private static final long serialVersionUID = 7009375306837374901L;
	private static final Log LOG = LogFactory.getLog(PartitioningConf.class);

	private final List<Partition> list;

	public PartitioningConf() {
		this.list = new ArrayList<Partition>(8);
	}

	@Nonnull
	public List<Partition> getPartitions() {
		return list;
	}

	public void loadSettings(@Nonnull String uri) throws SqletException {
		if (uri.endsWith(".csv") || uri.endsWith(".CSV")) {
			loadSettingsFromCsv(uri, list);
		} else {
			throw new IllegalArgumentException("Unsupported URI: " + uri);
		}
	}

	private static void loadSettingsFromCsv(String uri, List<Partition> list)
			throws SqletException {
		final InputStream is;
		try {
			FileSystemManager fsManager = VFS.getManager();
			FileObject fileObj = fsManager.resolveFile(uri);
			FileContent fileContent = fileObj.getContent();
			is = fileContent.getInputStream();
		} catch (FileSystemException e) {
			throw new SqletException(SqletErrorType.configFailed,
					"failed to load a file: " + uri, e);
		}
		InputStreamReader reader = new InputStreamReader(
				new FastBufferedInputStream(is));
		HeaderAwareCsvReader csvReader = new HeaderAwareCsvReader(reader, ',',
				'"');

		final Map<String, Integer> headerMap;
		try {
			headerMap = csvReader.parseHeader();
		} catch (IOException e) {
			throw new SqletException(SqletErrorType.configFailed,
					"failed to parse a header: " + uri, e);
		}

		final int[] fieldIndexes = toFieldIndexes(headerMap);
		final Map<GridNode, Partition> masterSlave = new HashMap<GridNode, Partition>(
				128);
		while (csvReader.next()) {
			String nodeStr = csvReader.get(fieldIndexes[0]);
			String masterStr = csvReader.get(fieldIndexes[1]);
			String dbUrl = csvReader.get(fieldIndexes[2]);
			String user = csvReader.get(fieldIndexes[3]);
			String password = csvReader.get(fieldIndexes[4]);
			String mapOutput = csvReader.get(fieldIndexes[5]);

			Preconditions.checkNotNull(nodeStr, dbUrl);

			GridNode node = GridUtils.getNode(nodeStr);
			Partition p = new Partition(node, dbUrl, user, password, mapOutput);
			if (masterStr == null || masterStr.length() == 0) {
				masterSlave.put(node, p);
				list.add(p);
			} else {
				GridNode master = GridUtils.getNode(masterStr);
				Partition masterPartition = masterSlave.get(master);
				if (masterPartition == null) {
					LOG.error("Master partition is not found for slave: " + p);
				} else {
					masterPartition.addSlave(p);
				}
			}
		}
	}

	private static int[] toFieldIndexes(@Nullable Map<String, Integer> map) {
		if (map == null) {
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

	public static final class Partition implements Serializable {
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
		@Nullable
		List<Partition> slaves;

		Partition(@Nonnull GridNode node, @Nonnull String dbUrl,
				@Nullable String user, @Nullable String password,
				@Nullable String mapOutput) {
			super();
			this.node = node;
			this.dbUrl = dbUrl;
			this.user = user;
			this.password = password;
			this.mapOutput = mapOutput;
		}

		public void addSlave(Partition slave) {
			if (slaves == null) {
				this.slaves = new LinkedList<Partition>();
			}
			slaves.add(slave);
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

		@Override
		public String toString() {
			return "Partition [node=" + node + ", dbUrl=" + dbUrl + ", user="
					+ user + ", password=" + password + ", mapOutput="
					+ mapOutput + ", slaves=" + slaves + "]";
		}

	}

	@Override
	public String toString() {
		return list.toString();
	}

}
