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

/**
 * This is a representation of a range of bytes on an encrypted blob, typically used during a download operation. This type is
 * immutable to ensure thread-safety of requests, so changing the values for a different operation requires construction
 * of a new object. Passing null as a EncryptedBlobRange value will default to the entire range of the blob.
 */
final class EncryptedBlobRange {

    /**
     * The calculated BlobRange
     */
    private BlobRange originalRange;

    /**
     * Offset from beginning of BlobRange, 0-31.
     */
    private long adjustedOffset;

    /**
     * How many bytes to decrypt. Must be greater than or equal to 0 if specified.
     */
    private Long adjustedCount;

    EncryptedBlobRange(BlobRange originalRange) {
        if(originalRange == null) {
            this.originalRange = new BlobRange();
            return;
        }

        int encryptionBlockSize = 16;
        long offset = originalRange.offset();
        Long count = originalRange.count();

        // calculate adjusted adjustedOffset
        if(originalRange.offset() != 0) {

            // align with increment of 16
            if(originalRange.offset() % encryptionBlockSize != 0) {
                long diff = offset % encryptionBlockSize;
                offset -= diff;

                // increment adjustedCount if necessary
                if(count != null) {
                    count += diff;
                }
            }

            // account for IV
            if(offset >= encryptionBlockSize) {
                offset -= encryptionBlockSize;

                // increment adjustedCount if necessary
                if(count != null) {
                    count += encryptionBlockSize;
                }
            }
        }

        // align adjustedCount with increment of 16
        if(count != null) {
            count += encryptionBlockSize - (count % encryptionBlockSize);
        }

        this.originalRange = new BlobRange().withOffset(offset).withCount(count);
        this.adjustedOffset = originalRange.offset() - offset;
        this.adjustedCount = originalRange.count();
    }

    /**
     * @return The calculated {@link BlobRange}
     */
    public BlobRange blobRange() {
        return this.originalRange;
    }

    /**
     * @return Offset from beginning of BlobRange, 0-31.
     */
    public long adjustedOffset() {
        return this.adjustedOffset;
    }

    /**
     * @return How many bytes to include in the range. Must be greater than or equal to 0 if specified.
     */
    public Long adjustedCount() {
        return this.adjustedCount;
    }

    /**
     * Sets the {@link BlobRange} for this EncryptedBlobRange.
     *
     * @param blobRange
     *          A {@link BlobRange}
     *
     * @return this
     */
    public EncryptedBlobRange withBlobRange(BlobRange blobRange) {
        this.originalRange = blobRange;
        return this;
    }

    /**
     * Sets of adjustedOffset for this EncryptedBlobRange.
     *
     * @param adjustedOffset
     *          Offset from beginning of BlobRange
     *
     * @return this
     */
    public EncryptedBlobRange withAdjustedOffset(long adjustedOffset) {
        if (adjustedOffset < 0) {
            throw new IllegalArgumentException("EncryptedBlobRange adjustedOffset must be greater than or equal to 0.");
        }
        this.adjustedOffset = adjustedOffset;
        return this;
    }

    /**
     * Sets the adjustedCount for this EncryptedBlobRange.
     *
     * @param adjustedCount
     *          The number of bytes to decrypt.
     *
     * @return this
     */
    public EncryptedBlobRange withAdjustedCount(Long adjustedCount) {
        if (adjustedCount != null && adjustedCount < 0) {
            throw new IllegalArgumentException(
                    "EncryptedBlobRange adjustedCount must be greater than or equal to 0 if specified.");
        }
        this.adjustedCount = adjustedCount;
        return this;
    }
}
