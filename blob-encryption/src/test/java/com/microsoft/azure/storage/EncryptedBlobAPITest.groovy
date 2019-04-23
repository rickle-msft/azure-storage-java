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

package com.microsoft.azure.storage

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.type.CollectionType
import com.microsoft.azure.keyvault.cryptography.SymmetricKey
import com.microsoft.azure.storage.blob.*
import com.microsoft.azure.storage.blob.encryption.BlobEncryptionPolicy
import com.microsoft.azure.storage.blob.encryption.Constants
import com.microsoft.azure.storage.blob.encryption.EncryptedBlobURL
import com.microsoft.azure.storage.blob.encryption.EncryptedBlockBlobURL
import com.microsoft.azure.storage.blob.encryption.TestEncryptionBlob
import com.microsoft.azure.storage.blob.models.BlobGetPropertiesResponse
import com.microsoft.azure.storage.blob.models.BlobHTTPHeaders
import com.microsoft.azure.storage.blob.models.BlockBlobCommitBlockListResponse
import com.microsoft.azure.storage.blob.models.LeaseAccessConditions
import com.microsoft.azure.storage.blob.models.ModifiedAccessConditions
import com.microsoft.azure.storage.blob.models.PageRange
import com.microsoft.azure.storage.blob.models.StorageErrorCode
import com.microsoft.rest.v2.util.FlowableUtil
import io.reactivex.Flowable
import spock.lang.Unroll

import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.security.MessageDigest

class EncryptedBlobAPITest extends APISpec {
    String keyId
    SymmetricKey symmetricKey
    BlobEncryptionPolicy blobEncryptionPolicy
    String blobName
    BlobURL blobURL
    PageBlobURL pageBlobURL
    AppendBlobURL appendBlobURL
    BlockBlobURL blockBlobURL
    EncryptedBlobURL encryptedBlobURL
    EncryptedBlockBlobURL encryptedBlockBlobURL

    def setup() {
        keyId = "keyId"
        KeyGenerator keyGen = KeyGenerator.getInstance("AES")
        keyGen.init(256)
        SecretKey secretKey = keyGen.generateKey()
        symmetricKey = new SymmetricKey(keyId, secretKey.getEncoded())

        blobEncryptionPolicy = new BlobEncryptionPolicy(symmetricKey, null, false)

        blobName = generateBlobName()
        blobURL = cu.createBlobURL(blobName)
        blockBlobURL = cu.createBlockBlobURL(blobName)
        pageBlobURL = cu.createPageBlobURL(blobName)
        appendBlobURL = cu.createAppendBlobURL(blobName)
        encryptedBlockBlobURL = new EncryptedBlockBlobURL(blockBlobURL, blobEncryptionPolicy)
    }

    def "Encryption not a no-op"() {
        when:
        ByteBuffer byteBuffer = getRandomData(Constants.KB)
        Flowable<ByteBuffer> flowable = Flowable.just(byteBuffer)

        encryptedBlockBlobURL.upload(flowable).blockingGet()

        DownloadResponse downloadResponse = blockBlobURL.download().blockingGet()

        ByteBuffer outputByteBuffer = FlowableUtil.collectBytesInBuffer(downloadResponse.body(null)).blockingGet()

        then:
        outputByteBuffer.array() != byteBuffer.array()
    }

    @Unroll
    def "Encryption"() {
        when:
        ByteBuffer byteBuffer = getRandomData(size)
        ByteBuffer[] byteBufferArray = new ByteBuffer[byteBufferCount]

        /*
        Sending a sequence of buffers allows us to test encryption behavior in different cases when the buffers do
        or do not align on encryption boundaries.
         */
        for (def i = 0; i < byteBufferCount; i++) {
            byteBufferArray[i] = ByteBuffer.wrap(Arrays.copyOfRange(
                    byteBuffer.array(), i * (int) (size / byteBufferCount), (int) ((i + 1) * (size / byteBufferCount))))
        }
        Flowable<ByteBuffer> flowable = Flowable.fromArray(byteBufferArray)

        BlockBlobCommitBlockListResponse uploadResponse = encryptedBlockBlobURL.upload(flowable).blockingGet()

        DownloadResponse downloadResponse = encryptedBlockBlobURL.download().blockingGet()

        ByteBuffer outputByteBuffer = FlowableUtil.collectBytesInBuffer(downloadResponse.body(null)).blockingGet()

        then:
        uploadResponse.statusCode() == 201
        downloadResponse.statusCode() == 200
        byteBuffer == outputByteBuffer

        where:
        size    | byteBufferCount
        10      | 1                 // 0 One buffer smaller than an encryption block.
        10      | 2                 // 1 A buffer that spans an encryption block.
        16      | 1                 // 2 A buffer exactly the same size as an encryption block.
        16      | 2                 // 3 Two buffers the same size as an encryption block.
        20      | 1                 // 4 One buffer larger than an encryption block.
        20      | 2                 // 5 Two buffers larger than an encryption block.
        100     | 1                 // 6 One buffer containing multiple encryption blocks
        5 * KB  | KB                // 7 Large number of small buffers.
        10 * MB | 2                 // 8 Small number of large buffers.
    }

    @Unroll
    def "Encryption headers"() {
        setup:
        BlobHTTPHeaders headers = new BlobHTTPHeaders().withBlobCacheControl(cacheControl)
                .withBlobContentDisposition(contentDisposition)
                .withBlobContentEncoding(contentEncoding)
                .withBlobContentLanguage(contentLanguage)
                .withBlobContentMD5(contentMD5)
                .withBlobContentType(contentType)

        when:
        encryptedBlockBlobURL.upload(defaultFlowable, headers, null, null).blockingGet()
        BlobGetPropertiesResponse response = encryptedBlockBlobURL.getProperties().blockingGet()

        then:
        response.statusCode() == 200
        validateBlobHeaders(response.headers(), cacheControl, contentDisposition, contentEncoding, contentLanguage,
                contentMD5, contentType == null ? "application/octet-stream" : contentType)
        // HTTP default content type is application/octet-stream

        where:
        cacheControl | contentDisposition | contentEncoding | contentLanguage | contentMD5                                                   | contentType
        null         | null               | null            | null            | null                                                         | null
        "control"    | "disposition"      | "encoding"      | "language"      | MessageDigest.getInstance("MD5").digest(defaultData.array()) | "type"
    }

    @Unroll
    def "Encryption metadata"() {
        setup:
        Metadata metadata = new Metadata()
        if (key1 != null) {
            metadata.put(key1, value1)
        }
        if (key2 != null) {
            metadata.put(key2, value2)
        }

        when:
        encryptedBlockBlobURL.upload(defaultFlowable, null, metadata, null).blockingGet()
        BlobGetPropertiesResponse response = encryptedBlockBlobURL.getProperties().blockingGet()

        then:
        response.statusCode() == 200
        response.headers().metadata() == metadata

        where:
        key1  | value1 | key2   | value2
        null  | null   | null   | null
        "foo" | "bar"  | "fizz" | "buzz"
    }

    @Unroll
    def "Encryption AC"() {
        setup:
        encryptedBlockBlobURL.upload(defaultFlowable).blockingGet()
        match = setupBlobMatchCondition(encryptedBlockBlobURL, match)
        leaseID = setupBlobLeaseCondition(encryptedBlockBlobURL, leaseID)
        BlobAccessConditions bac = new BlobAccessConditions().withModifiedAccessConditions(
                new ModifiedAccessConditions().withIfModifiedSince(modified).withIfUnmodifiedSince(unmodified)
                        .withIfMatch(match).withIfNoneMatch(noneMatch))
                .withLeaseAccessConditions(new LeaseAccessConditions().withLeaseId(leaseID))

        expect:
        encryptedBlockBlobURL.upload(defaultFlowable, null, null, bac).blockingGet().statusCode() == 201

        where:
        modified | unmodified | match        | noneMatch   | leaseID
        null     | null       | null         | null        | null
        oldDate  | null       | null         | null        | null
        null     | newDate    | null         | null        | null
        null     | null       | receivedEtag | null        | null
        null     | null       | null         | garbageEtag | null
        null     | null       | null         | null        | receivedLeaseID
    }

    @Unroll
    def "Encryption AC fail"() {
        setup:
        encryptedBlockBlobURL.upload(defaultFlowable).blockingGet()
        noneMatch = setupBlobMatchCondition(encryptedBlockBlobURL, noneMatch)
        setupBlobLeaseCondition(encryptedBlockBlobURL, leaseID)
        BlobAccessConditions bac = new BlobAccessConditions().withModifiedAccessConditions(
                new ModifiedAccessConditions().withIfModifiedSince(modified).withIfUnmodifiedSince(unmodified)
                        .withIfMatch(match).withIfNoneMatch(noneMatch))
                .withLeaseAccessConditions(new LeaseAccessConditions().withLeaseId(leaseID))

        when:
        encryptedBlockBlobURL.upload(defaultFlowable, null, null, bac).blockingGet()

        then:
        def e = thrown(StorageException)
        e.errorCode() == StorageErrorCode.CONDITION_NOT_MET ||
                e.errorCode() == StorageErrorCode.LEASE_ID_MISMATCH_WITH_BLOB_OPERATION
        where:
        modified | unmodified | match       | noneMatch    | leaseID
        newDate  | null       | null        | null         | null
        null     | oldDate    | null        | null         | null
        null     | null       | garbageEtag | null         | null
        null     | null       | null        | receivedEtag | null
        null     | null       | null        | null         | garbageLeaseID
    }

    // Progress tests. Tests with parallel upload/download. Stream retries.

    // Options: Pass a policy and no key on encryption--throw; no encryption policy in construction-> throw
    // Construct a policy and require encryption=true and no key or resolver -> throw

    // TODO: Document which tests are testing which cases. Ensure that some don't align along blocks. Maybe have a mock flowable that returns some really smally byteBuffers.
    // Request one byte. Test key resolver. Lots more. Require encryption tests (and downloading blobs that aren't encryption, esp. ones that are smaller than what the expanded range would try).
    // Samples. API refs. Reliable download.
    // Test EncryptedBlobRange
    // One blob sample is failing in the onErrorResumeNext case because it gets weird with the generics

    @Unroll
    def "Small blob tests"(int offset, Integer count, int size, int status) {
        when:
        ByteBuffer byteBuffer = getRandomData(size)

        Flowable<ByteBuffer> flowable = Flowable.just(byteBuffer)

        BlockBlobCommitBlockListResponse uploadResponse = encryptedBlockBlobURL.upload(
                flowable, null, null, null).blockingGet()


        DownloadResponse downloadResponse = encryptedBlockBlobURL.download(
                new BlobRange().withOffset(offset.longValue()).withCount(count), null, false, null).blockingGet()

        ByteBuffer outputByteBuffer = FlowableUtil.collectBytesInBuffer(
                downloadResponse.body(null)).blockingGet()

        and:
        def limit
        if (count != null) {
            if (count < byteBuffer.capacity()) {
                limit = offset + count
            } else {
                limit = byteBuffer.capacity()
            }
        } else {
            limit = size
        }
        byteBuffer.position(offset).limit(limit) // reset the position after the read in upload.
        then:
        uploadResponse.statusCode() == 201
        downloadResponse.statusCode() == status
        byteBuffer == outputByteBuffer

        where:
        offset | count | size | status // note
        0      | null  | 10   | 200 // 0
        3      | null  | 10   | 200 // 1
        0      | 10    | 10   | 206 // 2
        0      | 16    | 10   | 206 // 3
        3      | 16    | 10   | 206 // 4
        0      | 7     | 10   | 206 // 5
        3      | 7     | 10   | 206 // 6
        3      | 3     | 10   | 206 // 7
        0      | null  | 16   | 200 // 8
        5      | null  | 16   | 200 // 9
        0      | 16    | 16   | 206 // 10
        0      | 20    | 16   | 206 // 11
        5      | 20    | 16   | 206 // 12
        5      | 11    | 16   | 206 // 13
        5      | 7     | 16   | 206 // 14
        0      | null  | 24   | 200 // 15
        5      | null  | 24   | 200 // 16
        0      | 24    | 24   | 206 // 17
        5      | 24    | 24   | 206 // 18
        0      | 30    | 24   | 206 // 19
        5      | 19    | 24   | 206 // 20
        5      | 10    | 24   | 206 // 21
    }

    // Keep the small and large blob tests but combine them into Range tests. Looks like some pattern of:
    // Small, full blob, no offst; small, full blob, offset; small full blob count; ... blob size of block... blob bigger than block... etc.

    @Unroll
    def "Large Blob Tests"() {
        when:
        ByteBuffer byteBuffer = getRandomData(size)

        Flowable<ByteBuffer> flowable = Flowable.just(byteBuffer)

        BlockBlobCommitBlockListResponse uploadResponse = encryptedBlockBlobURL.upload(
                flowable, null, null, null).blockingGet()

        DownloadResponse downloadResponse = encryptedBlockBlobURL.download(
                new BlobRange().withOffset(offset.longValue()).withCount(count), null, false, null).blockingGet()

        ByteBuffer outputByteBuffer = FlowableUtil.collectBytesInBuffer(
                downloadResponse.body(null)).blockingGet()

        byte[] expectedByteArray = Arrays.copyOfRange(byteBuffer.array(), (int) offset, (int) (calcUpperBound(offset, count, size)))

        then:
        outputByteBuffer.array() == expectedByteArray

        where:
        offset          | count             | size          // note
        0L              | null              | 20 * KB       // 0
        5L              | null              | 20 * KB       // 1
        16L             | null              | 20 * KB       // 2
        24L             | null              | 20 * KB       // 3
        500             | null              | 20 * KB       // 4
        5000            | null              | 20 * KB       // 5
        0L              | 5L                | 20 * KB       // 6
        0L              | 16L               | 20 * KB       // 7
        0L              | 24L               | 20 * KB       // 8
        0L              | 500L              | 20 * KB       // 9
        0L              | 5000L             | 20 * KB       // 10
        0L              | 25 * KB           | 20 * KB       // 11
        0L              | 20 * KB           | 20 * KB       // 12
        5L              | 25 * KB           | 20 * KB       // 13
        5L              | 20 * KB - 5       | 20 * KB       // 14
        5L              | 20 * KB - 10      | 20 * KB       // 15
        5L              | 20 * KB - 20      | 20 * KB       // 16
        16L             | 20 * KB - 16      | 20 * KB       // 17
        16L             | 20 * KB           | 20 * KB       // 18
        16L             | 20 * KB - 20      | 20 * KB       // 19
        16L             | 20 * KB - 32      | 20 * KB       // 20
        500L            | 500L              | 20 * KB       // 21
        500L            | 20 * KB - 500     | 20 * KB       // 22
        20 * KB - 5     | 5                 | 20 * KB       // 23
        0L              | null              | 20 * KB + 8   // 24
        5L              | null              | 20 * KB + 8   // 25
        16L             | null              | 20 * KB + 8   // 26
        24L             | null              | 20 * KB + 8   // 27
        500             | null              | 20 * KB + 8   // 28
        5000            | null              | 20 * KB + 8   // 29
        0L              | 5L                | 20 * KB + 8   // 30
        0L              | 16L               | 20 * KB + 8   // 31
        0L              | 24L               | 20 * KB + 8   // 32
        0L              | 500L              | 20 * KB + 8   // 33
        0L              | 5000L             | 20 * KB + 8   // 34
        0L              | 20 * KB + 8       | 20 * KB + 8   // 35
        0L              | 20 * KB + 8       | 20 * KB + 8   // 36
        5L              | 20 * KB + 8 - 5   | 20 * KB + 8   // 37
        5L              | 20 * KB + 8 - 5   | 20 * KB + 8   // 38
        5L              | 20 * KB + 8 - 10  | 20 * KB + 8   // 39
        5L              | 20 * KB + 8 - 20  | 20 * KB + 8   // 40
        16L             | 20 * KB + 8 - 16  | 20 * KB + 8   // 41
        16L             | 20 * KB + 8       | 20 * KB + 8   // 42
        16L             | 20 * KB + 8 - 20  | 20 * KB + 8   // 43
        16L             | 20 * KB + 8 - 32  | 20 * KB + 8   // 44
        500L            | 500L              | 20 * KB + 8   // 45
        500L            | 20 * KB + 8 - 500 | 20 * KB + 8   // 46
        20 * KB + 8 - 5 | 5                 | 20 * KB + 8   // 47
    }

    @Unroll
    def "Block block cross platform decryption tests"() {
        when:
        List<TestEncryptionBlob> list = getTestData("json/blockBlob.json")
        symmetricKey = new SymmetricKey("symmKey1", Base64.getDecoder().decode(list.get(index).getKey()))
        blobEncryptionPolicy = new BlobEncryptionPolicy(symmetricKey, null, false)
        encryptedBlockBlobURL = new EncryptedBlockBlobURL(blockBlobURL, blobEncryptionPolicy)

        byte[] encryptedBytes = Base64.getDecoder().decode(list.get(index).getEncryptedContent())
        byte[] decryptedBytes = Base64.getDecoder().decode(list.get(index).getDecryptedContent())

        Metadata metadata = new Metadata()

        ObjectMapper objectMapper = new ObjectMapper()
        metadata.put(Constants.ENCRYPTION_DATA_KEY, objectMapper.writeValueAsString(list.get(index).getEncryptionData()))

        blockBlobURL.upload(Flowable.just(ByteBuffer.wrap(encryptedBytes)), encryptedBytes.length, null, metadata, null, null).blockingGet()

        DownloadResponse downloadResponse = encryptedBlockBlobURL.download(
                null, null, false, null).blockingGet()

        ByteBuffer outputByteBuffer = FlowableUtil.collectBytesInBuffer(
                downloadResponse.body(null)).blockingGet()

        then:
        outputByteBuffer.array() == decryptedBytes

        where:
        index << [0, 1, 2, 3, 4]
    }

    @Unroll
    def "Page blob decryption tests"() {
        when:
        TestEncryptionBlob testEncryptionBlob = getTestData("json/pageBlob.json").get(0)
        symmetricKey = new SymmetricKey("symmKey1", Base64.getDecoder().decode(testEncryptionBlob.getKey()))
        blobEncryptionPolicy = new BlobEncryptionPolicy(symmetricKey, null, false)
        encryptedBlobURL = new EncryptedBlobURL(pageBlobURL, blobEncryptionPolicy)

        byte[] encryptedBytes = Base64.getDecoder().decode(testEncryptionBlob.getEncryptedContent())
        byte[] decryptedBytes = Base64.getDecoder().decode(testEncryptionBlob.getDecryptedContent())

        Metadata metadata = new Metadata()

        ObjectMapper objectMapper = new ObjectMapper()
        metadata.put(Constants.ENCRYPTION_DATA_KEY, objectMapper.writeValueAsString(testEncryptionBlob.getEncryptionData()))

        pageBlobURL.create(encryptedBytes.length).blockingGet()

        pageBlobURL.setMetadata(metadata).blockingGet()

        pageBlobURL.uploadPages((new PageRange()).withStart(0).withEnd(encryptedBytes.length - 1),
                Flowable.just(ByteBuffer.wrap(encryptedBytes))).blockingGet()

        DownloadResponse downloadResponse = encryptedBlobURL.download(
                (new BlobRange()).withOffset(offset).withCount(count), null, false, null).blockingGet()

        ByteBuffer outputByteBuffer = FlowableUtil.collectBytesInBuffer(
                downloadResponse.body(null)).blockingGet()

        byte[] expectedByteArray = Arrays.copyOfRange(decryptedBytes, (int) offset, (int) (calcUpperBound(offset, count, decryptedBytes.length)))

        then:
        outputByteBuffer.array() == expectedByteArray

        where:
        offset | count             // note
        0      | null              // 0
        0      | 8                 // 1
        0      | 16                // 2
        0      | 24                // 3
        0      | 500               // 4
        0      | 16 * 1024         // 5
        0      | 16 * 1024 + 5     // 6
        0      | 17 * 1024         // 7
        8      | null              // 8
        8      | 8                 // 9
        8      | 16                // 10
        8      | 24                // 11
        8      | 500               // 12
        8      | 16 * 1024 - 8     // 13
        8      | 16 * 1024 - 16    // 14
        8      | 16 * 1024 + 8     // 15
        8      | 17 * 1024         // 16
        16     | null              // 17
        16     | 8                 // 18
        16     | 16                // 19
        16     | 24                // 20
        16     | 500               // 21
        16     | 16 * 1024 - 16    // 22
        16     | 16 * 1024 - 24    // 23
        16     | 16 * 1024         // 24
        16     | 17 * 1024         // 25
        24     | null              // 26
        24     | 8                 // 27
        24     | 16                // 28
        24     | 24                // 29
        24     | 500               // 30
        24     | 16 * 1024 - 24    // 31
        24     | 16 * 1024 - 32    // 32
        24     | 16 * 1024         // 33
        24     | 17 * 1024         // 34
        500    | null              // 25
        500    | 8                 // 36
        500    | 16                // 37
        500    | 24                // 38
        500    | 500               // 39
        500    | 16 * 1024 - 500   // 40
        500    | 16 * 1024 - 508   // 41
        500    | 16 * 1024 - 516   // 42
        500    | 16 * 1024         // 43
        500    | 17 * 1024         // 44
    }

    def "Append block decryption test"() {
        when:
        TestEncryptionBlob testEncryptionBlob = getTestData("json/appendBlob.json").get(0)
        symmetricKey = new SymmetricKey("symmKey1", Base64.getDecoder().decode(testEncryptionBlob.getKey()))
        blobEncryptionPolicy = new BlobEncryptionPolicy(symmetricKey, null, false)
        encryptedBlobURL = new EncryptedBlobURL(appendBlobURL, blobEncryptionPolicy)

        byte[] encryptedBytes = Base64.getDecoder().decode(testEncryptionBlob.getEncryptedContent())
        byte[] decryptedBytes = Base64.getDecoder().decode(testEncryptionBlob.getDecryptedContent())

        Metadata metadata = new Metadata()

        ObjectMapper objectMapper = new ObjectMapper()
        metadata.put(Constants.ENCRYPTION_DATA_KEY, objectMapper.writeValueAsString(testEncryptionBlob.getEncryptionData()))

        appendBlobURL.create().blockingGet()

        appendBlobURL.setMetadata(metadata).blockingGet()

        appendBlobURL.appendBlock(Flowable.just(ByteBuffer.wrap(encryptedBytes)), encryptedBytes.length).blockingGet()


        DownloadResponse downloadResponse = encryptedBlobURL.download(
                null, null, false, null).blockingGet()

        ByteBuffer outputByteBuffer = FlowableUtil.collectBytesInBuffer(
                downloadResponse.body(null)).blockingGet()

        then:
        outputByteBuffer.array() == decryptedBytes

    }

    def calcUpperBound(Long offset, Long count, Long size) {
        if (count == null || offset + count > size) {
            return size
        }
        return offset + count
    }

    def getTestData(String fileName) {
        Path path = Paths.get(getClass().getClassLoader().getResource(fileName).toURI())
        String json = new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
        ObjectMapper mapper = new ObjectMapper()
        CollectionType collectionType = mapper.getTypeFactory().constructCollectionType(List.class, TestEncryptionBlob.class)
        List<TestEncryptionBlob> list = mapper.readValue(json, collectionType)
        return list
    }
}
