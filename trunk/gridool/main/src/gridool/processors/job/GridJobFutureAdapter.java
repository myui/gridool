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
package gridool.processors.job;

import gridool.GridJobFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class GridJobFutureAdapter<R> implements GridJobFuture<R> {

    @Nullable
    private final Future<R> delegate;
    @Nullable
    private Throwable exception = null;

    private final boolean failed;

    public GridJobFutureAdapter(@Nonnull Future<R> delegate) {
        if(delegate == null) {
            throw new IllegalArgumentException();
        }
        this.delegate = delegate;
        this.failed = false;
    }

    public GridJobFutureAdapter(@Nonnull Throwable exception) {
        if(exception == null) {
            throw new IllegalArgumentException();
        }
        this.delegate = null;
        this.exception = exception;
        this.failed = true;
    }

    public void setException(Throwable ex) {
        this.exception = ex;
    }

    public R get() throws InterruptedException, ExecutionException {
        try {
            if(exception != null) {
                throw new ExecutionException(exception);
            }
            return delegate.get();
        } finally {
            // Harmless if task already completed
            if(delegate != null) {
                delegate.cancel(true); // interrupt if running
            }
        }
    }

    public R get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
            TimeoutException {
        if(exception != null) {
            throw new ExecutionException(exception);
        }
        return delegate.get(timeout, unit);
    }

    public boolean isCancelled() {
        if(failed) {
            return false;
        }
        return delegate.isCancelled();
    }

    public boolean isDone() {
        return failed || delegate.isDone();
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        if(isDone()) {
            return false;
        }
        return delegate.cancel(mayInterruptIfRunning);
    }

}
