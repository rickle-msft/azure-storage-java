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

import java.util.concurrent.atomic.AtomicLong;
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

    /*
    We need an AtomicLong to be able to update the value referenced. Because we are already synchronizing with the
    lock, we don't incur any additional performance hit here by the synchronization.
     */
    private AtomicLong totalProgress;

    ParallelProgressTracker(IProgressReceiver progressReceiver, Lock lock, AtomicLong totalProgress) {
        this.blockProgress = 0;
        this.progressReceiver = progressReceiver;
        this.transferLock = lock;
        this.totalProgress = totalProgress;
    }

    @Override
    public void reportProgress(long bytesTransferred) {
        this.blockProgress += bytesTransferred;
        System.out.println("Block progress: " + this.blockProgress);
        System.out.println("Bytes transferred: " + bytesTransferred);

        /*
        It is typically a bad idea to lock around customer code (which the progressReceiver is) because they could
        never release the lock. However, we have decided that it is sufficiently difficult for them to make their
        progressReporting code threadsafe that we will take that burden and the ensuing risks. Although it is the case
        that only one thread is allowed to be in onNext at once, however there are multiple independent requests
        happening at once to stage/download separate chunks, so we still need to lock either way.
         */
        transferLock.lock();
        this.totalProgress.getAndAdd(bytesTransferred);
        this.progressReceiver.reportProgress(this.totalProgress.get());
        transferLock.unlock();
    }

    /*
    This is used in the case of retries to rewind the amount of progress reported. It requires the support of a policy
    that is closer to the wire than the retry policy in order to anticipate whether a retry is imminent and rewind.
    Each try has a new instance of this object, so the state of this object will be lost upon resubscribing to the body
    in the case of a retry, which is why we have to preempt it.
     */
    public void rewindProgress() {
        transferLock.lock();
        this.totalProgress.addAndGet(-1 * this.blockProgress);
        transferLock.unlock();
    }
}
