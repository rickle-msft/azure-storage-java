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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The workflow for this type is as follows: A flowable represents a network stream. Some subscriber will be requesting
 * buffers from the pool, copying from the network stream to the buffer, and sending it off to be uploaded. The
 * buffering piece is strictly sequential and only has one worker. The upload can happen in parallel. This type makes
 * heavy use of the assumption that only one subscriber is requesting buffers at once and that they will be consumed
 * sequentially and not in parallel in any way. Once the buffer-worker finishes copying some amount of data and sends
 * off the data to be uploaded, it must signal to the pool it's ready to fill another buffer. The pool will then
 * try to emit one. If none is available, then the buffer-worker must wait until one of the upload finishes and the
 * uploader returns a buffer to the pool, at which point the pool will emit the returned buffer. The buffers emitted
 * are recycled, so it is up to the buffer-worker to call limit() to avoid uploading any trailing garbage data.
 */

// TODO: Remember to call limit when filling (so if I don't fill the whole buffer I don't read any garbage at the end) and then reset the position for reading.

// TODO: Change name and documentation to reflect "non-replayable flowable" instead of stream
final class UploadFromStreamBufferPool implements Publisher<ByteBuffer> {

    private final Queue<ByteBuffer> buffers;

    private final int maxBuffs;

    private int numBuffs = 0;

    private final int buffSize;

    /*
    It is possible that the request for a buffer (setting this flag to true) and the return of a buffer (potentially
    setting this flag to false) are on different threads and are happening at the same time.
     */
    private AtomicBoolean subscriberWaiting;

    private Subscriber<? super ByteBuffer> subscriber = null;

    UploadFromStreamBufferPool(int numBuffs, int buffSize) {
        buffers = new ArrayDeque<>();
        // Can't be less than 1, etc.
        this.maxBuffs = numBuffs;

        // Can't be less than 1, etc.
        this.buffSize = buffSize;

        subscriberWaiting = new AtomicBoolean(false);

        ByteBuffer buf = ByteBuffer.allocate(this.buffSize);
        this.numBuffs++;
        buffers.add(buf);
    }


    // Does calling onNext before someone subscribes lose the notification?
    public void returnBuffer(ByteBuffer b) {
        b.position(0);
        if (this.subscriberWaiting.get()) {
            this.subscriber.onNext(b);
            // TODO: Should this be an atomic getAndSet? Probably.
            this.subscriberWaiting.set(false);
        }
        else {
            this.buffers.add(b);
        }
    }

    // This signals to the pool that the worker copying the stream is ready to start copying into the next buffer.
    public void requestBuffer() {
        if (!this.buffers.isEmpty()) {
            ByteBuffer buff = this.buffers.peek();
            this.buffers.remove();
            this.subscriber.onNext(buff);
        }
        else if (this.numBuffs < this.maxBuffs) {
            ByteBuffer newBuf = ByteBuffer.allocate(this.buffSize);
            this.numBuffs++;
            this.subscriber.onNext(newBuf);
        }
        else {
            this.subscriberWaiting.set(true);
        }
    }

    void sourceComplete() {
        // Do I need to drain the queue so those buffers can be GC'd?
        this.subscriber.onComplete();
    }

    // TODO: Test by requesting max+1 times and never calling return. We should only get max.
    // Test by requesting max+n times and calling return n times. We should get max+n buffers.

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> s) {
        if (this.subscriber != null) {
            throw new IllegalStateException("Only one subscriber is permitted per pool to preserve concurrency " +
                    "assumptions");
        }
        this.subscriber = s;
        // This should always be true since we add to the list in the constructor and this gets called next.
        if (!this.buffers.isEmpty()) {
            s.onNext(buffers.element());
            buffers.remove();
        }
    }
}
