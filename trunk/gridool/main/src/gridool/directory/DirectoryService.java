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
package gridool.directory;

import gridool.GridConfiguration;
import gridool.GridException;
import gridool.GridResourceRegistry;
import gridool.GridService;
import gridool.directory.ILocalDirectory.DirectoryIndexType;
import gridool.locking.LockManager;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import xbird.storage.DbException;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class DirectoryService implements GridService {
    private static final Log LOG = LogFactory.getLog(DirectoryService.class);

    @Nullable
    private final ILocalDirectory directory;

    public DirectoryService(@Nonnull GridConfiguration config, @Nonnull GridResourceRegistry registry) {
        LockManager lockManager = registry.getLockManager();
        this.directory = createLocalDirectory(lockManager, config);
        registry.setDirectory(directory);
    }

    public String getServiceName() {
        return DirectoryService.class.getName();
    }

    public boolean isDaemon() {
        return false;
    }

    public void start() throws GridException {
        try {
            directory.start();
        } catch (DbException e) {
            LOG.error(e.getMessage(), e);
            throw new GridException(e.getMessage(), e.getCause());
        }
    }

    public void stop() throws GridException {
        try {
            directory.close();
        } catch (DbException e) {
            LOG.debug(e.getMessage(), e);
            throw new GridException(e.getMessage(), e.getCause());
        }
    }

    private static ILocalDirectory createLocalDirectory(LockManager lockManager, GridConfiguration config) {
        final DirectoryIndexType type = config.getDirectoryIndexType();
        final ILocalDirectory ld;
        switch(type) {
            case tcb:
                ld = new TcbLocalDirectory(lockManager);
                break;
            case bfile:
            default:
                ld = new DefaultLocalDirectory(lockManager);
                break;
        }
        return ld;
    }

}
