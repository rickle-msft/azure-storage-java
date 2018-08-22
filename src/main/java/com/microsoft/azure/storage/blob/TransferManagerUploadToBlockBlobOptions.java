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

public class TransferManagerUploadToBlockBlobOptions {

    /**
     * An object which represents the default parallel upload options. progressReceiver=null. httpHeaders, metadata,
     * and accessConditions are default values. parallelism=5.
     */
    public static final TransferManagerUploadToBlockBlobOptions DEFAULT =
            new TransferManagerUploadToBlockBlobOptions(null, null, null, null);

    private BlobHTTPHeaders httpHeaders;

    private Metadata metadata;

    private BlobAccessConditions accessConditions;

    private int parallelism;

    /**
     * Creates a new object that configures the parallel upload behavior. Null may be passed to accept the default
     * behavior.
     *
     * @param httpHeaders
     *      {@link BlobHTTPHeaders}
     * @param metadata
     *      {@link Metadata}
     * @param accessConditions
     *      {@link BlobAccessConditions}
     * @param parallelism
     *      A {@code int} that indicates the maximum number of blocks to upload in parallel. Must be greater than 0.
     */
    public TransferManagerUploadToBlockBlobOptions(BlobHTTPHeaders httpHeaders, Metadata metadata,
            BlobAccessConditions accessConditions, Integer parallelism) {
        if (parallelism == null) {
            this.parallelism = 5;
        }
        else if (parallelism <= 0) {
            throw new IllegalArgumentException("Parallelism must be > 0");
        } else {
            this.parallelism = parallelism;
        }

        this.httpHeaders = httpHeaders;
        this.metadata = metadata;
        this.accessConditions = accessConditions == null ? BlobAccessConditions.NONE : accessConditions;
    }

    /**
     * @return
     *      {@link BlobHTTPHeaders}
     */
    public BlobHTTPHeaders getHttpHeaders() {
        return httpHeaders;
    }

    /**
     * @return
     *      {@link Metadata}
     */
    public Metadata getMetadata() {
        return metadata;
    }

    /**
     * @return
     *      {@link BlobAccessConditions}
     */
    public BlobAccessConditions getAccessConditions() {
        return accessConditions;
    }

    /**
     * @return
     *      A {@code int} that indicates the maximum number of blocks to upload in parallel. Must be greater than 0.
     */
    public int getParallelism() {
        return parallelism;
    }
}
