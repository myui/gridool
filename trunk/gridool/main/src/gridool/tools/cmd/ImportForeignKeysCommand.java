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
package gridool.tools.cmd;

import gridool.Grid;
import gridool.GridClient;
import gridool.db.partitioning.monetdb.ImportForeignKeysJob;

import java.rmi.RemoteException;

import xbird.util.cmdline.CommandBase;
import xbird.util.cmdline.CommandException;
import xbird.util.cmdline.Option.BooleanOption;
import xbird.util.cmdline.Option.StringOption;
import xbird.util.struct.Pair;

/**
 * -templateDb <dbname> -gzip true import foreign keys
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405+xbird@gmail.com)
 */
public final class ImportForeignKeysCommand extends CommandBase {

    public ImportForeignKeysCommand() {
        addOption(new StringOption("templateDb", "jdbc:monetdb://localhost/templatedb", true));
        addOption(new BooleanOption("gzip", true, false));
    }

    public boolean match(String[] args) {
        if(args.length != 3) {
            return false;
        }
        if(!"import".equalsIgnoreCase(args[0])) {
            return false;
        }
        if(!"foreign".equalsIgnoreCase(args[1])) {
            return false;
        }
        if(!"keys".equalsIgnoreCase(args[2])) {
            return false;
        }
        return true;
    }

    public boolean process(String[] args) throws CommandException {
        String templateDbName = getOption("templateDb");
        Boolean useGzip = getOption("gzip");
        final Pair<String, Boolean> jobParams = new Pair<String, Boolean>(templateDbName, useGzip);
        final Grid grid = new GridClient();
        try {
            grid.execute(ImportForeignKeysJob.class, jobParams);
        } catch (RemoteException e) {
            throwException(e.getMessage());
            return false;
        }
        return true;
    }

    public String usage() {
        return constructHelp("Import foreign keys", "-templateDb <dbname> [-gzip <true/false>] import foreign keys");
    }

}
