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

import io.reactivex.Flowable;

import java.nio.ByteBuffer;

/**
 * RESERVED FOR INTERNAL USE. Represents the encrypted blob and its encrypted data
 */
public final class EncryptedBlob {

    /**
     * {@link EncryptionData} to decrypt EncryptedBlob
     */
    private final EncryptionData encryptedData;

    /**
     * The encrypted blob content as a Flowable ByteBuffer
     */
    private final Flowable<ByteBuffer> byteBufferFlowable;

    /**
     * Creates a new EncryptedBlob with given EncryptedData and Flowable ByteBuffer
     *
     * @param encryptedData
     *          A {@link EncryptionData}
     * @param byteBufferFlowable
     *          A Flowable ByteBuffer
     */
    public EncryptedBlob(EncryptionData encryptedData, Flowable<ByteBuffer> byteBufferFlowable) {
        this.encryptedData = encryptedData;
        this.byteBufferFlowable = byteBufferFlowable;
    }

    /**
     * @return This EncryptedBlob's EncryptedData.
     */
    public EncryptionData getEncryptionData() {
        return this.encryptedData;
    }

    /**
     * @return This EncryptedBlob's Flowable ByteBuffer.
     */
    public Flowable<ByteBuffer> getByteBufferFlowable() {
        return this.byteBufferFlowable;
    }
}
