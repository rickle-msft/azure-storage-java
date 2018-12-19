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

package com.microsoft.azure.storage.blob.encryption;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.storage.blob.*;
import com.microsoft.azure.storage.blob.models.*;
import com.microsoft.rest.v2.Context;
import io.reactivex.Flowable;
import io.reactivex.Single;

import java.nio.ByteBuffer;
import java.security.InvalidKeyException;

/**
 * Represents a URL to an encrypted block blob. It may be obtained by direct construction. This class does not hold any
 * state about a particular blob but is instead a convenient way of sending off appropriate requests to the resource on
 * the service. Please refer to the
 * <a href=https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs>Azure Docs</a>
 * for more information on block blobs.
 *
 */
public final class EncryptedBlockBlobURL extends EncryptedBlobURL {

    /**
     * Default block size for uploads.
     */
    public static final int BLOB_DEFAULT_UPLOAD_BLOCK_SIZE = 4 * Constants.MB;

    /**
     * Default maximum number of blocks to upload in parallel.
     */
    public static final Integer UPLOAD_PARALLELISM = 5;

    /**
     * Creates a EncryptedBlockBlobURL with given BlockBlobURL and BlobEncryptionPolicy
     *
     * @param blockBlobURL
     *          A {@link BlockBlobURL}
     * @param blobEncryptionPolicy
     *          A {@link BlobEncryptionPolicy}
     */
    public EncryptedBlockBlobURL(
            BlockBlobURL blockBlobURL, BlobEncryptionPolicy blobEncryptionPolicy) {
        super(blockBlobURL, blobEncryptionPolicy);
    }

    /**
     * Creates a new encrypted block blob, or updates the content of an existing block blob.
     * Updating an existing block blob overwrites any existing metadata on the blob. Partial updates are not
     * supported; the content of the existing blob is overwritten with the new content.
     *
     * @param data
     *      The data to write to the blob.
     * @param headers
     *      {@link BlobHTTPHeaders}
     * @param metadata
     *      {@link Metadata}
     * @param accessConditions
     *      {@link BlobAccessConditions}
     * @param blockSize
     *      The size of each block that will be staged. This value also determines the size that each buffer used by
     *      this method will be and determines the number of requests that need to be made. The amount of memory
     *      consumed by this method may be up to blockSize * numBuffers. If block size is large, this method will
     *      make fewer network calls, but each individual call will send more data and will therefore take longer.
     * @param numBuffers
     *      The maximum number of buffers this method should allocate. Must be at least two. Generally this value
     *      should have some relationship to the value for parallelism passed via the options. If the number of
     *      available buffers is smaller than the level of parallelism, then this method will not be able to make
     *      full use of the available parallelism. It is unlikely that the value need be more than two times the
     *      level of parallelism as such a value means that (assuming buffering is fast enough) there are enough
     *      available buffers to have both one occupied for each worker and one ready for all workers should they
     *      all complete the current request at approximately the same time. The amount of memory consumed by this
     *      method may be up to blockSize * numBuffers.
     * @param parallelism
     *       A {@code int} that indicates the maximum number of blocks to upload in parallel. Must be greater than 0.
     *       May be null to accept default behavior.
     * @param progressReceiver
     *       {@link IProgressReceiver}
     *
     * @return
     *      Emits the successful response.
     */
    public Single<BlockBlobCommitBlockListResponse> upload(Flowable<ByteBuffer> data, BlobHTTPHeaders headers,
            Metadata metadata, BlobAccessConditions accessConditions, int blockSize, int numBuffers, Integer parallelism,
            IProgressReceiver progressReceiver) throws InvalidKeyException {
        return this.blobEncryptionPolicy.encryptBlob(data).flatMap(encryptedBlob -> {

            // Put encryptionData in metadata
            Metadata md = metadata;
            if(md == null) {
                md = new Metadata();
            }
            ObjectMapper objectMapper = new ObjectMapper();
            String encryptionDataString = objectMapper.writeValueAsString(encryptedBlob.getEncryptionData());
            md.put(Constants.ENCRYPTION_DATA_KEY, encryptionDataString);

            // Build uploadOptions
            TransferManagerUploadToBlockBlobOptions uploadOptions = new TransferManagerUploadToBlockBlobOptions(
                    progressReceiver, headers, md, accessConditions, parallelism);

            return TransferManager.uploadFromNonReplayableFlowable(encryptedBlob.getByteBufferFlowable(),
                    this.toBlockBlobURL(), blockSize, numBuffers, uploadOptions);
        });
    }

    /**
     * Creates a new encrypted block blob, or updates the content of an existing block blob using default
     * block size, number of buffers, and parallelism.  Updating an existing block blob overwrites any
     * existing metadata on the blob. Partial updates are not supported; the content of the existing blob
     * is overwritten with the new content.
     *
     * @param data
     *      The data to write to the blob.
     * @param headers
     *      {@link BlobHTTPHeaders}
     * @param metadata
     *      {@link Metadata}
     * @param accessConditions
     *      {@link BlobAccessConditions}
     *
     * @return
     *      Emits the successful response.
     */
    public Single<BlockBlobCommitBlockListResponse> upload(Flowable<ByteBuffer> data, BlobHTTPHeaders headers,
            Metadata metadata, BlobAccessConditions accessConditions) throws InvalidKeyException {
        return this.upload(data, headers, metadata, accessConditions, BLOB_DEFAULT_UPLOAD_BLOCK_SIZE,
                UPLOAD_PARALLELISM + 1, UPLOAD_PARALLELISM, null);
    }
}
