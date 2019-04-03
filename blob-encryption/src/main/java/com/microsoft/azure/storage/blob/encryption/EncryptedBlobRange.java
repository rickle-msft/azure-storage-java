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

import com.microsoft.azure.storage.blob.BlobRange;

import static com.microsoft.azure.storage.blob.encryption.Constants.ENCRYPTION_BLOCK_SIZE;

/**
 * This is a representation of a range of bytes on an encrypted blob, which may be expanded from the requested range to
 * included extra data needed for encryption. Note that this type is not strictly thread-safe as the download method
 * will update the count in case the user did not specify one. Passing null as an EncryptedBlobRange value will default
 * to the entire range of the blob.
 */
final class EncryptedBlobRange {

    /**
     * The BlobRange passed by the customer and the range we must actually return.
     */
    private BlobRange originalRange;

    /**
     * Amount the beginning of the range, 0-31, needs to be adjusted in order to align along an encryption block
     * boundary and include the IV.
     */
    private int offsetAdjustment;

    /**
     * How many bytes to download, including the adjustments for encryption block boundaries and the IV.
     * Must be greater than or equal to 0 if specified.
     */
    private Long adjustedDownloadCount;

    EncryptedBlobRange(BlobRange originalRange) {
        if(originalRange == null) {
            this.originalRange = new BlobRange();
            return;
        }

        this.originalRange = originalRange;
        this.offsetAdjustment = 0;
        this.adjustedDownloadCount = this.originalRange.count();

        // Calculate offsetAdjustment.
        if(originalRange.offset() != 0) {

            // Align with encryption block boundary.
            if(originalRange.offset() % ENCRYPTION_BLOCK_SIZE != 0) {
                long diff = this.originalRange.offset() % ENCRYPTION_BLOCK_SIZE;
                this.offsetAdjustment += diff;
                if(this.adjustedDownloadCount != null) {
                    this.adjustedDownloadCount += diff;
                }
            }

            // Account for IV.
            if(this.originalRange.offset() >= ENCRYPTION_BLOCK_SIZE) {
                this.offsetAdjustment += ENCRYPTION_BLOCK_SIZE;
                // Increment adjustedDownloadCount if necessary.
                if(this.adjustedDownloadCount != null) {
                    this.adjustedDownloadCount += ENCRYPTION_BLOCK_SIZE;
                }
            }
        }

        /*
        Align adjustedDownloadCount with encryption block boundary at the end of the range. Note that it is impossible
        to adjust past the end of the blob as an encrypted blob was padded to align to an encryption block boundary.
         */
        if(this.adjustedDownloadCount != null) {
            this.adjustedDownloadCount += ENCRYPTION_BLOCK_SIZE -
                    (int)(this.adjustedDownloadCount % ENCRYPTION_BLOCK_SIZE);
        }

    }

    /**
     * @return The calculated {@link BlobRange}
     */
    BlobRange originalRange() {
        return this.originalRange;
    }

    /**
     * @return Offset from beginning of BlobRange, 0-31.
     */
    int offsetAdjustment() {
        return this.offsetAdjustment;
    }

    /**
     * @return How many bytes to include in the range. Must be greater than or equal to 0 if specified.
     */
    Long adjustedDownloadCount() {
        return this.adjustedDownloadCount;
    }

    /**
     * @param count
     *         The adjustedDownloadCount
     */
    void withAdjustedDownloadCount(long count) {
        this.adjustedDownloadCount = count;
    }

    /**
     * For convenient interaction with the blobURL.download() method.
     *
     * @return
     *        A {@link BlobRange} object which includes the necessary adjustments to offset and count to effectively
     *        decrypt the blob.
     */
    BlobRange toBlobRange() {
        return new BlobRange().withCount(this.adjustedDownloadCount)
                .withOffset(this.originalRange.offset() - this.offsetAdjustment);
    }
}
