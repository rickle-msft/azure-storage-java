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

import io.reactivex.*;
import io.reactivex.functions.BiConsumer;
import io.reactivex.functions.Function;

import java.io.IOException;
import java.lang.Error;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Base64;
import java.util.UUID;

import static java.lang.StrictMath.toIntExact;

/**
 * This class contains a collection of methods (and structures associated with those methods) which perform higher-level
 * operations. Whereas operations on the URL types guarantee a single REST request and make no assumptions on desired
 * behavior, these methods will often compose several requests to provide a convenient way of performing more complex
 * operations. Further, we will make our own assumptions and optimizations for common cases that may not be ideal for
 * rarer cases.
 */
public class TransferManager {

    /**
     * Uploads the contents of a file to a block blob in parallel, breaking it into block-size chunks if necessary.
     *
     * @apiNote
     * ## Sample Code \n
     * [!code-java[Sample_Code](../azure-storage-java/src/test/java/com/microsoft/azure/storage/Samples.java?name=tm_file "Sample code for TransferManager.uploadFileToBlockBlob")] \n
     * For more samples, please see the [Samples file](https://github.com/Azure/azure-storage-java/blob/New-Storage-SDK-V10-Preview/src/test/java/com/microsoft/azure/storage/Samples.java)
     *
     * @param file
     *      The file to upload.
     * @param blockBlobURL
     *      Points to the blob to which the data should be uploaded.
     * @param blockLength
     *      If the data must be broken up into blocks, this value determines what size those blocks will be. This will
     *      affect the total number of service requests made. This value will be ignored if the data can be uploaded in
     *      a single put-blob operation. Must be between 1 and {@link BlockBlobURL#MAX_STAGE_BLOCK_BYTES}. Note as well
     *      that {@code fileLength/blockLength} must be less than or equal to {@link BlockBlobURL#MAX_BLOCKS}.
     * @param options
     *      {@link TransferManagerUploadToBlockBlobOptions}
     * @return
     *      Emits the successful response.
     */
    public static Single<CommonRestResponse> uploadFileToBlockBlob(
            final FileChannel file, final BlockBlobURL blockBlobURL, final int blockLength,
            final TransferManagerUploadToBlockBlobOptions options) {
        Utility.assertNotNull("file", file);
        Utility.assertNotNull("blockBlobURL", blockBlobURL);
        Utility.assertInBounds("blockLength", blockLength, 1, BlockBlobURL.MAX_STAGE_BLOCK_BYTES);
        TransferManagerUploadToBlockBlobOptions optionsReal = options == null ? TransferManagerUploadToBlockBlobOptions.DEFAULT : options;

        try {
            // If the size of the file can fit in a single upload, do it this way.
            if (file.size() < BlockBlobURL.MAX_PUT_BLOB_BYTES) {
                return doSingleShotUpload(
                        Flowable.just(file.map(FileChannel.MapMode.READ_ONLY, 0, file.size())), file.size(),
                        blockBlobURL, optionsReal);
            }

            int numBlocks = calculateNumBlocks(file.size(), blockLength);
            return Observable.range(0, numBlocks)
                    .map((Function<Integer, ByteBuffer>) i -> {
                        /*
                        The docs say that the result of mapping a region which is not entirely contained by the file
                        is undefined, so we must be precise with the last block size.
                         */
                        int count = Math.min(blockLength, (int)(file.size()-i*blockLength));
                        // Memory map the file to get a ByteBuffer to an in memory portion of the file.
                        return file.map(FileChannel.MapMode.READ_ONLY, i*blockLength, count);
                    })
                    // Gather all of the buffers, in order, into this list, which will become the block list.
                    .collectInto(new ArrayList<>(numBlocks),
                            (BiConsumer<ArrayList<ByteBuffer>, ByteBuffer>) ArrayList::add)
                    // Turn the list into a call to uploadByteBuffersToBlockBlob and return that result.
                    .flatMap(blocks ->
                            uploadByteBuffersToBlockBlob(blocks, blockBlobURL, optionsReal));
        }
        catch (IOException e) {
            throw new Error(e);
        }
    }

    private static int calculateNumBlocks(long dataSize, int blockLength) {
        // Can successfully cast to an int because MaxBlockSize is an int, which this expression must be less than.
        int numBlocks = toIntExact(dataSize/blockLength);
        // Include an extra block for trailing data.
        if (dataSize%blockLength != 0) {
            numBlocks++;
        }
        return numBlocks;
    }

    /**
     * Uploads a large ByteBuffer to a block blob in parallel, breaking it up into block-size chunks if necessary.
     *
     * @apiNote
     * ## Sample Code \n
     * [!code-java[Sample_Code](../azure-storage-java/src/test/java/com/microsoft/azure/storage/Samples.java?name=tm_buffer "Sample code for TransferManager.uploadByteBufferToBlockBlob")] \n
     * For more samples, please see the [Samples file](https://github.com/Azure/azure-storage-java/blob/New-Storage-SDK-V10-Preview/src/test/java/com/microsoft/azure/storage/Samples.java)
     *
     * @param data
     *      The buffer to upload.
     * @param blockBlobURL
     *      A {@link BlockBlobURL} that points to the blob to which the data should be uploaded.
     * @param blockLength
     *      If the data must be broken up into blocks, this value determines what size those blocks will be. This will
     *      affect the total number of service requests made. This value will be ignored if the data can be uploaded in
     *      a single put-blob operation.
     * @param options
     *      {@link TransferManagerUploadToBlockBlobOptions}
     * @return
     *      Emits the successful response.
     */
    public static Single<CommonRestResponse> uploadByteBufferToBlockBlob(
            final ByteBuffer data, final BlockBlobURL blockBlobURL, final int blockLength,
            final TransferManagerUploadToBlockBlobOptions options) {
        Utility.assertNotNull("data", data);
        Utility.assertNotNull("blockBlobURL", blockBlobURL);
        Utility.assertInBounds("blockLength", blockLength, 1, BlockBlobURL.MAX_STAGE_BLOCK_BYTES);
        TransferManagerUploadToBlockBlobOptions optionsReal = options == null ? TransferManagerUploadToBlockBlobOptions.DEFAULT : options;

        // If the size of the buffer can fit in a single upload, do it this way.
        if (data.remaining() < BlockBlobURL.MAX_PUT_BLOB_BYTES) {
            return doSingleShotUpload(Flowable.just(data), data.remaining(), blockBlobURL, optionsReal);
        }

        int numBlocks = calculateNumBlocks(data.remaining(), blockLength);

        return Observable.range(0, numBlocks)
                .map(i -> {
                    int count = Math.min(blockLength, data.remaining()-i*blockLength);
                    ByteBuffer block = data.duplicate();
                    block.position(i*blockLength);
                    block.limit(i*blockLength+count);
                    return block;
                })
                .collectInto(new ArrayList<>(numBlocks),
                        (BiConsumer<ArrayList<ByteBuffer>, ByteBuffer>) ArrayList::add)
                .flatMap(blocks -> uploadByteBuffersToBlockBlob(blocks, blockBlobURL, optionsReal));
    }

    /**
     * Uploads an iterable of {@code ByteBuffers} to a block blob. The data will first data will first be examined to
     * check the size and validate the number of blocks. If the total amount of data in all the buffers is small enough
     * (i.e. less than or equal to {@link BlockBlobURL#MAX_PUT_BLOB_BYTES}, this method will perform a single upload
     * operation. Otherwise, each {@code ByteBuffer} in the iterable is assumed to be its own discreet block of data for
     * the block blob and will be uploaded as such. Note that in this case, each ByteBuffer must be less than or equal
     * to {@link BlockBlobURL#MAX_STAGE_BLOCK_BYTES}. Note as well that there can only be up to
     * {@link BlockBlobURL#MAX_BLOCKS} ByteBuffers in the list.
     *
     * @apiNote
     * ## Sample Code \n
     * [!code-java[Sample_Code](../azure-storage-java/src/test/java/com/microsoft/azure/storage/Samples.java?name=tm_buffers "Sample code for TransferManager.uploadByteBuffersToBlockBlob")] \n
     * For more samples, please see the [Samples file](https://github.com/Azure/azure-storage-java/blob/New-Storage-SDK-V10-Preview/src/test/java/com/microsoft/azure/storage/Samples.java)
     *
     * @param data
     *      The data to upload.
     * @param blockBlobURL
     *      A {@link BlockBlobURL} that points to the blob to which the data should be uploaded.
     * @param options
     *      {@link TransferManagerUploadToBlockBlobOptions}
     * @return
     *      Emits the successful response.
     */
    public static Single<CommonRestResponse> uploadByteBuffersToBlockBlob(
            final Iterable<ByteBuffer> data, final BlockBlobURL blockBlobURL,
            final TransferManagerUploadToBlockBlobOptions options) {
        Utility.assertNotNull("data", data);
        Utility.assertNotNull("blockBlobURL", blockBlobURL);
        TransferManagerUploadToBlockBlobOptions optionsReal = options == null ? TransferManagerUploadToBlockBlobOptions.DEFAULT : options;

        // Determine the size of the blob and the number of blocks
        long size = 0;
        int numBlocks = 0;
        for (ByteBuffer b : data) {
            size += b.remaining();
            numBlocks++;
        }

        // If the size can fit in 1 upload call, do it this way.
        if (size <= BlockBlobURL.MAX_PUT_BLOB_BYTES) {
            return doSingleShotUpload(Flowable.fromIterable(data), size, blockBlobURL, optionsReal);
        }

        if (numBlocks > BlockBlobURL.MAX_BLOCKS) {
            throw new IllegalArgumentException(SR.BLOB_OVER_MAX_BLOCK_LIMIT);
        }

        // Generate an observable that emits items which are the ByteBuffers in the provided Iterable.
        return Observable.fromIterable(data)
                /*
                 For each ByteBuffer, make a call to stageBlock as follows. concatMap ensures that the items
                 emitted by this Observable are in the same sequence as they are begun, which will be important for
                 composing the list of Ids later.
                 */
                .concatMapEager(blockData -> {
                    if (blockData.remaining() > BlockBlobURL.MAX_STAGE_BLOCK_BYTES) {
                        throw new IllegalArgumentException(SR.INVALID_BLOCK_SIZE);
                    }

                    final String blockId = Base64.getEncoder().encodeToString(
                            UUID.randomUUID().toString().getBytes());

                    /*
                     Make a call to stageBlock. Instead of emitting the response, which we don't care about other than
                     that it was successful, emit the blockId for this request. These will be collected below. Turn that
                     into an Observable which emits one item to comply with the signature of concatMapEager.
                     */
                    return blockBlobURL.stageBlock(blockId, Flowable.just(blockData),
                            blockData.remaining(), optionsReal.getAccessConditions().getLeaseAccessConditions())
                            .map(x -> blockId).toObservable();

                /*
                 Specify the number of concurrent subscribers to this map. This determines how many concurrent rest
                 calls are made. This is so because maxConcurrency is the number of internal subscribers available to
                 subscribe to the Observables emitted by the source. A subscriber is not released for a new subscription
                 until its Observable calls onComplete, which here means that the call to stageBlock is finished. Prefetch
                 is a hint that each of the Observables emitted by the source will emit only one value, which is true
                 here because we have converted from a Single.
                 */

                }, optionsReal.getParallelism(), 1)
                /*
                collectInto will gather each of the emitted blockIds into a list. Because we used concatMap, the Ids
                will be emitted according to their block number, which means the list generated here will be properly
                ordered. This also converts into a Single.
                */
                .collectInto(new ArrayList<>(numBlocks), (BiConsumer<ArrayList<String>, String>) ArrayList::add)
                /*
                collectInto will not emit the list until its source calls onComplete. This means that by the time we
                call stageBlock list, all of the stageBlock calls will have finished. By flatMapping the list, we can
                "map" it into a call to commitBlockList.
                 */
                .flatMap( ids ->
                        blockBlobURL.commitBlockList(ids, optionsReal.getHttpHeaders(), optionsReal.getMetadata(),
                                optionsReal.getAccessConditions()))
                /*
                Finally, we must turn the specific response type into a CommonRestResponse by mapping.
                 */
                .map(CommonRestResponse::createFromPutBlockListResponse);

    }

    private static Single<CommonRestResponse> doSingleShotUpload(
            Flowable<ByteBuffer> data, long size, BlockBlobURL blockBlobURL, TransferManagerUploadToBlockBlobOptions options) {

        // Transform the specific RestResponse into a CommonRestResponse.
        return blockBlobURL.upload(data, size, options.getHttpHeaders(),
                options.getMetadata(), options.getAccessConditions())
                .map(CommonRestResponse::createFromPutBlobResponse);
    }
}
