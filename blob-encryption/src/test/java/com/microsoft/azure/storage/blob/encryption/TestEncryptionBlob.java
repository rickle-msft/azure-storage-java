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

import com.microsoft.azure.storage.blob.encryption.EncryptionData;

public class TestEncryptionBlob {
    private String key;
    private EncryptionData encryptionData;
    private String decryptedContent;
    private String encryptedContent;

    public TestEncryptionBlob() {}

    public TestEncryptionBlob(String key, EncryptionData encryptionData, String decryptedContent, String encryptedContent) {
        this.key = key;
        this.encryptionData = encryptionData;
        this.decryptedContent = decryptedContent;
        this.encryptedContent = encryptedContent;
    }

    public String getKey() { return this.key; }

    public EncryptionData getEncryptionData() { return this.encryptionData; }

    public String getEncryptedContent() { return this.encryptedContent; }

    public String getDecryptedContent() { return  this.decryptedContent; }
}
