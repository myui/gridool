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
package gridool.db.helpers;

import gridool.Settings;
import gridool.util.io.FileUtils;
import gridool.util.primitive.Primitives;

import java.io.File;
import java.io.IOException;

import javax.annotation.Nonnull;

import nl.cwi.monetdb.merovingian.Control;
import nl.cwi.monetdb.merovingian.MerovingianException;
import nl.cwi.monetdb.merovingian.SabaothDB;
import nl.cwi.monetdb.merovingian.SabaothDB.SABdbState;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class MonetDBUtils {
    private static final Log LOG = LogFactory.getLog(MonetDBUtils.class);

    private static final int mero_controlport;
    private static final String merovingian_pass;
    static {
        mero_controlport = Primitives.parseInt(Settings.get("gridool.db.monetdb.mero_controlport"), 50001);
        String pass = Settings.get("gridool.db.monetdb.merovingian_pass");
        if(pass == null) {
            String fp = Settings.get("gridool.db.monetdb.merovingian_pass.file");
            if(fp != null) {
                File file = new File(fp);
                if(file.exists()) {
                    if(file.canRead()) {
                        pass = FileUtils.toString(file).trim();
                    }
                }
            }
        }
        merovingian_pass = pass;
    }

    private MonetDBUtils() {}

    public static boolean restartLocalDbInstance(@Nonnull String connectUrl) {
        if(merovingian_pass == null) {
            throw new IllegalArgumentException("`gridool.db.monetdb.merovingian_pass' is not set");
        }
        final String dbname = getDbName(connectUrl);
        final Control ctrl = new Control("localhost", mero_controlport, merovingian_pass);
        final SabaothDB db;
        try {
            db = ctrl.getStatus(dbname);
        } catch (MerovingianException e) {
            LOG.error("failed to get status: " + dbname, e);
            return false;
        } catch (IOException ie) {
            LOG.error("failed to get status: " + dbname, ie);
            return false;
        }
        SabaothDB.SABdbState state = db.getState();
        if(state == SABdbState.SABdbRunning) {
            try {
                ctrl.stop(dbname);
            } catch (MerovingianException me) {
                LOG.error("failed to stop database: " + dbname, me);
                return false;
            } catch (IOException ie) {
                LOG.error("failed to stop database: " + dbname, ie);
                return false;
            }
        } else {
            LOG.warn("MonetDB instance `" + dbname + "' is in an unexpected state: " + state);
        }
        try {
            ctrl.start(dbname);
        } catch (MerovingianException me) {
            LOG.error("failed to start database: " + dbname, me);
            return false;
        } catch (IOException ie) {
            LOG.error("failed to start database: " + dbname, ie);
            return false;
        }
        if(LOG.isInfoEnabled()) {
            LOG.info("Restarted MonetDB instance: " + dbname);
        }
        return true;
    }

    public static String getDbName(@Nonnull String connectUrl) {
        return connectUrl.substring(connectUrl.lastIndexOf('/') + 1);
    }

}
