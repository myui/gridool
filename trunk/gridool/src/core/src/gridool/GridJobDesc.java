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
package gridool;

import gridool.util.io.IOUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public class GridJobDesc implements Externalizable {

    @Nonnull
    private/* final */String jobClass;
    @Nonnull
    private/* final */List<URL> classSearchPaths;

    public GridJobDesc() {
        this.classSearchPaths = new ArrayList<URL>(4);
    } // for Externalizable

    public GridJobDesc(@Nonnull Class<? extends GridJob<?, ?>> jobClass) {
        this(jobClass.getName());
    }

    public GridJobDesc(@Nonnull String jobClass) {
        this.jobClass = jobClass;
        this.classSearchPaths = new ArrayList<URL>(4);
        setDefaultSearchPaths();
    }

    private void setDefaultSearchPaths() {
        String conf = Settings.get("gridool.libjars");
        if(conf != null) {
            String[] paths = conf.split(";");
            for(String path : paths) {
                if(path != null) {
                    try {
                        URL url = new URL(path);
                        classSearchPaths.add(url);
                    } catch (MalformedURLException e) {
                        ;
                    }
                }
            }
        }
    }

    @Nonnull
    public String getJobClass() {
        return jobClass;
    }

    @Nonnull
    public List<URL> getClassSearchPaths() {
        return classSearchPaths;
    }

    public void addClassSearchPaths(URL... urls) {
        for(URL url : urls) {
            if(url != null) {
                classSearchPaths.add(url);
            }
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.jobClass = IOUtils.readString(in);
        final int numUrls = in.readInt();
        for(int i = 0; i < numUrls; i++) {
            URL url = (URL) in.readObject();
            classSearchPaths.add(url);
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(jobClass, out);
        final int numUrls = classSearchPaths.size();
        out.writeInt(numUrls);
        for(int i = 0; i < numUrls; i++) {
            URL url = classSearchPaths.get(i);
            out.writeObject(url);
        }
    }

}
