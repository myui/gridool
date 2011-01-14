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
package gridool.util.cmdline;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public abstract class CommandBase implements Command {

    protected final Map<String, Option<?>> options;

    public CommandBase() {
        options = new HashMap<String, Option<?>>(8);
    }

    public Map<String, Option<?>> listOptions() {
        return options;
    }

    protected final void addOption(Option<?> opt) {
        options.put(opt.getName().toLowerCase(), opt);
    }

    protected final void addOption(Option<?>... opts) {
        for(Option<?> opt : opts) {
            options.put(opt.getName().toLowerCase(), opt);
        }
    }

    @SuppressWarnings("unchecked")
    protected final <T> T getOption(@Nonnull String name) {
        final Option<?> opt = options.get(name.toLowerCase());
        if(opt == null) {
            return null;
        }
        return (T) opt.getValue();
    }

    protected final void throwException(String msg) throws CommandException {
        throw new CommandException(msg + "\n" + usage());
    }

    protected final void rethrowException(Exception cause) throws CommandException {
        this.rethrowException(cause.getMessage(), cause);
    }

    protected final void rethrowException(String msg, Exception cause) throws CommandException {
        throw new CommandException(msg + "\n" + usage(), cause);
    }

    protected static String constructHelp(String desc, String usage) {
        final StringBuilder buf = new StringBuilder(256);
        buf.append("------------------------------------------\n");
        buf.append("Description: ").append(desc).append('\n');
        buf.append("Usage: ").append(usage).append('\n');
        buf.append("------------------------------------------");
        return buf.toString();
    }
}
