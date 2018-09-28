/*
 * Copyright Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.azure.storage.blob;

import java.util.concurrent.locks.Lock;

/**
 * This type is used to keep track of the total amount of data transferred as a part of a parallel upload in order to
 * coordinate progress reporting to the end user. We need this additional type because we can't keep local state
 * directly as lambdas require captured local variables to be effectively final.
 */
final class ParallelProgressTracker implements IProgressReceiver{

    private long blockProgress;

    private final IProgressReceiver progressReceiver;

    /*
    This lock will be instantiated by the operation initiating the whole transfer to coordinate each
    ParallelProgressTracker.
     */
    private final Lock transferLock;

    private Long totalProgress;

    ParallelProgressTracker(IProgressReceiver progressReceiver, Lock lock, Long totalProgress) {
        this.blockProgress = 0;
        this.progressReceiver = progressReceiver;
        this.transferLock = lock;
        this.totalProgress = totalProgress;
    }

    @Override
    public void reportProgress(long bytesTransferred) {
        long diff = bytesTransferred - this.blockProgress;
        this.blockProgress =  bytesTransferred;

        /*
        It is typically a bad idea to lock around customer code (which the progressReceiver is) because they could
        never release the lock. However, we have decided that it is sufficiently difficult for them to make their
        progressReporting code threadsafe that we will take that burden and the ensuing risks.
         */
        // Is this necessary? Is it guaranteed that only one thread is in onNext at a time?
        transferLock.lock();
        this.totalProgress += diff;
        this.progressReceiver.reportProgress(this.totalProgress);
        transferLock.unlock();

    }
}
