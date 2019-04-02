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

import com.microsoft.azure.storage.blob.*;
import com.microsoft.azure.storage.blob.models.BlobDownloadHeaders;
import com.microsoft.azure.storage.blob.models.BlobType;
import com.microsoft.rest.v2.Context;
import com.microsoft.rest.v2.RestResponse;
import com.microsoft.rest.v2.http.HttpPipeline;
import io.reactivex.Flowable;
import io.reactivex.Single;

import java.net.URL;
import java.nio.ByteBuffer;

public class EncryptedBlobURL extends BlobURL {

    protected BlobEncryptionPolicy blobEncryptionPolicy;

    /**
     * Creates a {@code EncryptedBlobURL}
     *
     * @param url
     *          A {@code URL}
     * @param pipeline
     *          A {@code HttpPipeline}
     * @param blobEncryptionPolicy
     *          A {@code BlobEncryptionPolicy}
     */
    public EncryptedBlobURL(URL url, HttpPipeline pipeline, BlobEncryptionPolicy blobEncryptionPolicy) {
        super(url, pipeline);
        this.blobEncryptionPolicy = blobEncryptionPolicy;
    }

    /**
     * Creates a {@code EncryptedBlobURL}
     *
     * @param blobURL
     *          A {@link BlobURL}
     * @param blobEncryptionPolicy
     *          A {@link BlobEncryptionPolicy}
     */
    public EncryptedBlobURL(BlobURL blobURL, BlobEncryptionPolicy blobEncryptionPolicy) {
        super(blobURL.toURL(), blobURL.pipeline());
        this.blobEncryptionPolicy = blobEncryptionPolicy;
    }

    /**
     * Reads a range of bytes from a blob. The response also includes the blob's properties and metadata. For more
     * information, see the <a href="https://docs.microsoft.com/rest/api/storageservices/get-blob">Azure Docs</a>.
     * <p>
     * Note that the response body has reliable download functionality built in, meaning that a failed download stream
     * will be automatically retried. This behavior may be configured with {@link ReliableDownloadOptions}.
     *
     * @param range
     *         {@link BlobRange}
     * @param accessConditions
     *         {@link BlobAccessConditions}
     * @param rangeGetContentMD5
     *         Whether the contentMD5 for the specified blob range should be returned.
     * @param context
     *      {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *      {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *      arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *      immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *      parent, forming a linked list.
     *
     * @return Emits the successful response.
     * @apiNote ## Sample Code \n [!code-java[Sample_Code](../azure-storage-java/src/test/java/com/microsoft/azure/storage/Samples.java?name=upload_download
     * "Sample code for BlobURL.download")] \n For more samples, please see the [Samples
     * file](%https://github.com/Azure/azure-storage-java/blob/New-Storage-SDK-V10-Preview/src/test/java/com/microsoft/azure/storage/Samples.java)
     */
    @Override
    public Single<DownloadResponse> download(BlobRange range, BlobAccessConditions accessConditions,
            boolean rangeGetContentMD5, Context context) {

        final EncryptedBlobRange encryptedBlobRange = new EncryptedBlobRange(range);

        return super.download(encryptedBlobRange.toBlobRange(), accessConditions,
                rangeGetContentMD5, context)
                .map(downloadResponse -> {
                    if(downloadResponse.headers().contentLength() != null && downloadResponse.headers().contentLength()
                            <= encryptedBlobRange.offsetAdjustment()) {
                        throw new IllegalArgumentException("BlobRange offsetAdjustment exceeds the size of the blob");
                    }

                    /*
                    We need to be able to keep track of when we are at the end of the download, so we can finalize the
                    cipher, so we set the download count even if it wasn't set by the user.
                     */
                    encryptedBlobRange.withAdjustedDownloadCount(calculateCount(downloadResponse.headers()));

                    // Calculate padding
                    boolean padding = false;
                    /*
                    Page blob writes always align to 512 bytes, which a multiple of the encryption block size, so we
                    never need to pad.
                     */
                    if(downloadResponse.headers().blobType() == BlobType.PAGE_BLOB) {
                        padding = false;
                    }
                    // If our download includes the last encryption block of the blob, we need to account for padding.
                    else if(encryptedBlobRange.adjustedDownloadCount() >= blobSize(downloadResponse.headers()) - 16) {
                        padding = true;
                    }

                    Flowable<ByteBuffer> decryptedFlowable
                            = this.blobEncryptionPolicy.decryptBlob(downloadResponse.headers().metadata(),
                            downloadResponse.body(new ReliableDownloadOptions().withMaxRetryRequests(0)),
                            encryptedBlobRange, padding);

                    RestResponse<BlobDownloadHeaders, Flowable<ByteBuffer>> restResponse = new RestResponse<>(
                            downloadResponse.rawResponse().request(),
                            downloadResponse.rawResponse().statusCode(),
                            downloadResponse.rawResponse().headers(),
                            downloadResponse.rawResponse().rawHeaders(),
                            decryptedFlowable);

                    return new DownloadResponse(restResponse, downloadResponse.info());
                });
    }

    private Long blobSize(BlobDownloadHeaders headers) {
        String range = headers.contentRange();
        return Long.valueOf(range.split("/")[1]);
    }

    private Long calculateCount(BlobDownloadHeaders headers) {
        String range = headers.contentRange();
        String upperIndexString = range.split("-")[1].split("/")[0];
        String lowerIndexString = range.split("-")[0].split(" ")[1];
        return Long.valueOf(upperIndexString) - Long.valueOf(lowerIndexString) + 1;
    }
}
