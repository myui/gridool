/*
 * @(#)$Id$
 *
 * Copyright 2010-2011 Makoto YUI
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


import java.util.HashMap;
import java.util.Map;

/**
 * @author Makoto YUI
 */
public final class SystemCatalog {
    public static final String DEFAULT_CATALOG_NAME = "default";

    private final Map<String, PartitioningConf> partitioningConfs;
    private final Map<String, MapReduceConf> mapredConfs;

    public SystemCatalog() {
        this.partitioningConfs = new HashMap<String, PartitioningConf>();
        this.mapredConfs = new HashMap<String, MapReduceConf>();
        partitioningConfs.put(DEFAULT_CATALOG_NAME, new PartitioningConf());
        mapredConfs.put(DEFAULT_CATALOG_NAME, new MapReduceConf());
    }

    public PartitioningConf getPartitioningConf(String catalogName) {
        return partitioningConfs.get(catalogName);
    }

    public void setPartitioningConf(String catalogName, PartitioningConf conf) {
        partitioningConfs.put(catalogName, conf);
    }

    public MapReduceConf getMapReduceConf(String catalogName) {
        return mapredConfs.get(catalogName);
    }

    public void setMapReduceConf(String catalogName, MapReduceConf conf) {
        mapredConfs.put(catalogName, conf);
    }

    public boolean deleteCatalog(String catalogName) {
        PartitioningConf pconf = partitioningConfs.remove(catalogName);
        MapReduceConf mconf = mapredConfs.remove(catalogName);
        return pconf != null && mconf != null;
    }

}
