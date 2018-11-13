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

import io.reactivex.Flowable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
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
final class UploadFromStreamBufferPool {

    private final BlockingQueue<ByteBuffer> buffers;

    private final int maxBuffs;

    private int numBuffs = 0;

    private final int buffSize;

    private ByteBuffer currentBuf;


    UploadFromStreamBufferPool(int numBuffs, int buffSize) {
        buffers = new LinkedBlockingQueue<>();
        // Can't be less than 1, etc.
        this.maxBuffs = numBuffs;

        // Can't be less than 1, etc.
        this.buffSize = buffSize;

        ByteBuffer buf = ByteBuffer.allocate(this.buffSize);
        this.numBuffs++;
        buffers.add(buf);
    }

    public Flowable<ByteBuffer> write(ByteBuffer buf) {
        if (this.currentBuf == null) {
            this.currentBuf = this.getBuffer();
        }

        Flowable<ByteBuffer> result;

        if (this.currentBuf.remaining() >= buf.remaining()) {
            this.currentBuf.put(buf);
            if (this.currentBuf.remaining() == 0) {
                this.currentBuf.position(0);
                result = Flowable.just(this.currentBuf);
                // This will force us to get a new buffer next time we try to write.
                this.currentBuf = null;
            }
            else {
                // We are still filling the current buffer.
                result = Flowable.empty();
            }
        }
        else {
            // Adjust the window of buf so that we fill up currentBuf.
            int oldLimit = buf.limit();
            buf.limit(buf.position() + this.currentBuf.remaining());
            this.currentBuf.put(buf);
            // Set the old limit so we can read the rest.
            buf.limit(oldLimit);

            // Reset the position so we can read the buffer.
            this.currentBuf.position(0);
            result =  Flowable.just(this.currentBuf);

            // Get a new buffer and fill it with whatever is left from buf. Note that this relies on the assumption that
            // the source Flowable has been split up into buffers that are no bigger than chunk size. This assumption
            // means we'll only have to over flow once, and the buffer we overflow into will not be filled.
            this.currentBuf = this.getBuffer();
            this.currentBuf.put(buf);
        }

        return result;
    }

    private ByteBuffer getBuffer() {
        if (this.numBuffs < this.maxBuffs && this.buffers.isEmpty()) {
            return ByteBuffer.allocate(this.buffSize);
        }
        try {
            return this.buffers.take();
        }
        catch (InterruptedException e) {
            throw new IllegalStateException("UploadFromStream thread interrupted.");
        }
    }

    // Where do I call this? Can't do it in onComplete... maybe in an andThen()
    Flowable<ByteBuffer> flush() {
        if (this.currentBuf != null) {
            this.currentBuf.limit(this.currentBuf.position());
            this.currentBuf.position(0);
            return Flowable.just(this.currentBuf);
        }
        // TODO: double check how I handle these ending conditions
        return Flowable.empty();
    }

    // Does calling onNext before someone subscribes lose the notification?
    void returnBuffer(ByteBuffer b) {
        b.position(0);

        try {
            this.buffers.put(b);
        }
        catch (InterruptedException e) {
            throw new IllegalStateException("UploadFromStream thread interrupted.");
        }
    }

    // TODO: Test by requesting max+1 times and never calling return. We should only get max.
    // Test by requesting max+n times and calling return n times. We should get max+n buffers.
}
