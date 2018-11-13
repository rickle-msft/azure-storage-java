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

package com.microsoft.azure.storage.blob

import com.microsoft.azure.storage.APISpec
import com.microsoft.rest.v2.util.FlowableUtil
import io.reactivex.Flowable
import io.reactivex.annotations.NonNull
import io.reactivex.functions.Function
import org.reactivestreams.Publisher

import java.nio.ByteBuffer
import java.util.concurrent.Callable

class UploadFromStreamBufferPoolTest extends APISpec{
    def "Buffer pool"() {
        // This pool expects that the buffers are pre-chunked and that the caller will call flush() at the end
        setup:
        def pool = new UploadFromStreamBufferPool(2, 10)
        def buffers = [getRandomData(7), getRandomData(7)]

        when:
        def data = Flowable.fromIterable(buffers)
                .flatMap(new Function<ByteBuffer, Publisher>() {
            @Override
            Publisher apply(@NonNull ByteBuffer byteBuffer) throws Exception {
                pool.write(byteBuffer)
            }
        })
        .concatWith(Flowable.defer(new Callable<Publisher>() {
            @Override
            Publisher call() throws Exception {
                return pool.flush();
            }
        }))

        then:
        compareListToBuffer(buffers, FlowableUtil.collectBytesInBuffer(data).blockingGet())
    }

    def compareListToBuffer(List<ByteBuffer> buffers, ByteBuffer result) {
        result.position(0)
        for (ByteBuffer buffer : buffers) {
            buffer.position(0)
            result.limit(result.position() + buffer.remaining())
            if (buffer != result) {
                return false
            }
            result.position(result.position() + buffer.remaining())
        }
        return result.remaining() == 0
    }

    // First one requires two pool buffers.
    // Bunch of buffers smaller than the pool buffers.
    // One big buffer.
}
