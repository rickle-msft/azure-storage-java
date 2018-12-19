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
import com.microsoft.azure.keyvault.core.IKey;
import com.microsoft.azure.keyvault.core.IKeyResolver;
import io.reactivex.Flowable;
import io.reactivex.Single;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a blob encryption policy that is used to perform envelope encryption/decryption of Azure storage blobs.
 */
public final class BlobEncryptionPolicy {
    /**
     * The {@link IKeyResolver} used to select the correct key for decrypting existing blobs.
     */
    private final IKeyResolver keyResolver;

    /**
     * An object of type {@link IKey} that is used to wrap/unwrap the content key during encryption.
     */
    private final IKey keyWrapper;

    /**
     * A value to indicate that the data read from the server should be encrypted.
     */
    private final boolean requireEncryption;

    /**
     * Initializes a new instance of the {@link BlobEncryptionPolicy} class with the specified key and resolver.
     * <p>
     * If the generated policy is intended to be used for encryption, users are expected to provide a key at the
     * minimum. The absence of key will cause an exception to be thrown during encryption. If the generated policy is
     * intended to be used for decryption, users can provide a keyResolver. The client library will - 1. Invoke the key
     * resolver if specified to get the key. 2. If resolver is not specified but a key is specified, match the key id on
     * the key and use it.
     *
     * @param key
     *            An object of type {@link IKey} that is used to wrap/unwrap the content encryption key.
     * @param keyResolver
     *            The key resolver used to select the correct key for decrypting existing blobs.
     * @param requireEncryption
     *            If set to true, decryptBlob() will throw an IllegalArgumentException if blob is not encrypted.
     */
    public BlobEncryptionPolicy(IKey key, IKeyResolver keyResolver, boolean requireEncryption) {
        this.keyWrapper = key;
        this.keyResolver = keyResolver;
        this.requireEncryption = requireEncryption;
    }

    /**
     * Encrypts given Flowable ByteBuffer.
     *
     * @param plainTextFlowable
     *          The Flowable ByteBuffer to be encrypted.
     *
     * @return A {@link EncryptedBlob}
     *
     * @throws InvalidKeyException
     */
    Single<EncryptedBlob> encryptBlob(Flowable<ByteBuffer> plainTextFlowable) throws InvalidKeyException {
        Utility.assertNotNull("key", this.keyWrapper);
        try {
            KeyGenerator keyGen = KeyGenerator.getInstance("AES");
            keyGen.init(256);

            Cipher cipher = Cipher.getInstance(Constants.AES_CBC_PKCS5PADDING);

            // Generate content encryption key
            SecretKey aesKey = keyGen.generateKey();
            cipher.init(Cipher.ENCRYPT_MODE, aesKey);

            Map<String, String> keyWrappingMetadata = new HashMap<>();
            keyWrappingMetadata.put(Constants.AGENT_METADATA_KEY, Constants.AGENT_METADATA_VALUE);

            return Single.fromFuture(this.keyWrapper.wrapKeyAsync(aesKey.getEncoded(), null /* algorithm */))
                    .map(encryptedKey -> {
                        WrappedKey wrappedKey = new WrappedKey(
                                this.keyWrapper.getKid(), encryptedKey.getKey(), encryptedKey.getValue());

                        // Build EncryptionData
                        EncryptionData encryptionData = new EncryptionData()
                                .withEncryptionMode(Constants.ENCRYPTION_MODE)
                                .withEncryptionAgent(
                                        new EncryptionAgent(Constants.ENCRYPTION_PROTOCOL_V1, EncryptionAlgorithm.AES_CBC_256))
                                .withKeyWrappingMetadata(keyWrappingMetadata)
                                .withContentEncryptionIV(cipher.getIV())
                                .withWrappedContentKey(wrappedKey);

                        // Encrypt plain text with content encryption key
                        Flowable<ByteBuffer> encryptedTextFlowable = plainTextFlowable.map(plainTextBuffer -> {
                            int outputSize = cipher.getOutputSize(plainTextBuffer.remaining());

                            // This should be the only place we allocate memory in encryptBlob()
                            ByteBuffer encryptedTextBuffer = ByteBuffer.allocate(outputSize);

                            int decryptedBytes = cipher.update(plainTextBuffer, encryptedTextBuffer);
                            encryptedTextBuffer.position(0);
                            encryptedTextBuffer.limit(decryptedBytes);
                            return encryptedTextBuffer;
                        });

                        // Defer() ensures the contained code is not executed until the Flowable is subscribed to, in
                        // other words, cipher.doFinal() will not be called until the plainTextFlowable has completed
                        // and therefore all other data has been encrypted.
                        encryptedTextFlowable = Flowable.concat(encryptedTextFlowable, Flowable.defer(() ->
                                Flowable.just(ByteBuffer.wrap(cipher.doFinal()))));
                        return new EncryptedBlob(encryptionData, encryptedTextFlowable);
                    });
        }
        catch(NoSuchAlgorithmException | NoSuchPaddingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Decrypted all or part of an encrypted Block-, Page- or AppendBlob.
     *
     * @param metadata
     *          The Blob's {@link com.microsoft.azure.storage.blob.Metadata}
     * @param encryptedFlowable
     *          The encrypted Flowable Bytebuffer to decrypt
     * @param encryptedBlobRange
     *          A {@link EncryptedBlobRange} indicating the range to decrypt
     * @param padding
     *          Boolean indicating if the padding mode should be set or not.
     *
     * @return A Flowable ByteBuffer that has been decrypted
     */
    Flowable<ByteBuffer> decryptBlob(Map<String, String> metadata, Flowable<ByteBuffer> encryptedFlowable,
                                            EncryptedBlobRange encryptedBlobRange, boolean padding) {
        // Throw if neither the key nor the key resolver are set.
        if(this.keyWrapper == null && this.keyResolver == null) {
            throw new IllegalArgumentException("Key and KeyResolver cannot both be null");
        }
        EncryptionData encryptionData = getAndValidateEncryptionData(metadata);

        // RequireEncryption == false and Blob is not encrypted
        if(encryptionData == null) {
            return encryptedFlowable;
        }

        // The number of bytes we have processed so far, not including the Cipher buffer
        AtomicLong processedBytes = new AtomicLong(0);

        return getKeyEncryptionKey(encryptionData)
                .flatMapPublisher(contentEncryptionKey -> {
                    // Calculate IV
                    byte[] IV = new byte[16];

                    // If we are at the beginning of the blob, grab the IV from the encryptionData.
                    if(encryptedBlobRange.blobRange().offset() == 0 && encryptedBlobRange.adjustedOffset() < 16) {
                        IV = encryptionData.contentEncryptionIV();
                    }

                    // Rx makes it difficult to grab the first 16 bytes of data to pass as an IV to the cipher.
                    // As a work around, we initialize the cipher with a garbage iv (empty byte array) and attempt to
                    // decrypt the first 16 bytes (the actual IV for the relevant data). We throw away this "decrypted"
                    // data. Now, though, because each block of 16 is used as the IV for the next, the original 16 bytes
                    // of downloaded data are in position to be used as the iv for the data actually requested and we are
                    // in the desired state

                    Cipher cipher = getCipher(contentEncryptionKey, encryptionData, IV, padding);

                    return encryptedFlowable.map(encryptedByteBuffer -> {
                        ByteBuffer plaintextByteBuffer;
                        int outputSize = cipher.getOutputSize(encryptedByteBuffer.remaining());

                        // If we could potentially decrypt more bytes than encryptedByteBuffer can hold
                        if(outputSize > encryptedByteBuffer.limit()) {
                            plaintextByteBuffer = ByteBuffer.allocate(outputSize);
                        }
                        else {
                            plaintextByteBuffer = encryptedByteBuffer.duplicate();
                        }

                        int decryptedBytes;

                        // This is the last ByteBuffer, since we will exceed the requested byte adjustedCount with this request.
                        if(processedBytes.longValue() + outputSize
                                >= encryptedBlobRange.blobRange().count()) {
                            decryptedBytes = cipher.doFinal(encryptedByteBuffer, plaintextByteBuffer);

                            // This is the first ByteBuffer, since we have not processed any bytes.
                            if(processedBytes.longValue() == 0) {
                                plaintextByteBuffer.position((int)(encryptedBlobRange.adjustedOffset()));
                            }
                            else {
                                plaintextByteBuffer.position(0);
                            }

                            // If we downloaded the whole blob, then there was no added range at the end, which means any
                            // extra bytes were just padding that were removed by the cipher. It is safe to simply set the
                            // limit of the buffer to be the number of bytes decrypted
                            if(encryptedBlobRange.adjustedCount() != null) {
                                int limit;

                                // This is the first ByteBuffer, since we have not processed any bytes.
                                if(processedBytes.longValue() == 0) {
                                    limit = (int)(plaintextByteBuffer.position() + encryptedBlobRange.adjustedCount());
                                }
                                else {
                                    limit = plaintextByteBuffer.position()
                                            + (int)(encryptedBlobRange.adjustedCount() - processedBytes.longValue() + encryptedBlobRange.adjustedOffset());
                                }

                                if(limit <= plaintextByteBuffer.capacity() && decryptedBytes >= limit) {
                                    plaintextByteBuffer.limit(limit);
                                }
                                else {
                                    plaintextByteBuffer.limit(decryptedBytes);
                                }
                            }
                            else {
                                plaintextByteBuffer.limit(decryptedBytes);
                            }
                            return plaintextByteBuffer;
                        }

                        // We are not on the last ByteBuffer
                        else {
                            decryptedBytes = cipher.update(encryptedByteBuffer, plaintextByteBuffer);

                            // We will not reach encryptedBlobRange.adjustedOffset() on this ByteBuffer, meaning we will not
                            // start decrypting actual data.
                            if(processedBytes.longValue() + decryptedBytes < encryptedBlobRange.adjustedOffset()) {
                                plaintextByteBuffer = ByteBuffer.allocate(0);
                                processedBytes.addAndGet(decryptedBytes);
                                return plaintextByteBuffer;
                            }
                            // We will reach encryptedBlobRange.adjustedOffset() in this ByteBuffer, meaning we will start
                            // decrypting user requested data
                            else if(processedBytes.longValue() <= encryptedBlobRange.adjustedOffset()) {

                                // Advance the position forward to exclude the offsetAdjustment, which the user did not actually
                                // ask for. The reason we subtract processedBytes is in the case where we started with a very small
                                // ByteBuffer. i.e offsetAdjustment is 30 and the first ByteBuffer was only 10. Then we would
                                // only want to advance the position of the current ByteBuffer by 20 to exclude the remaining
                                // offsetAdjustment instead of overshooting and excluding the first 30, which would exclude the
                                // start of the user's data.
                                plaintextByteBuffer.position((int)(encryptedBlobRange.adjustedOffset() - processedBytes.longValue()));
                            } else {
                                plaintextByteBuffer.position(0);
                            }

                            // Ensure the consumer can only read the amount of bytes we decrypted.
                            plaintextByteBuffer.limit(decryptedBytes);

                            // Increment processedBytes
                            processedBytes.addAndGet(decryptedBytes);

                            return plaintextByteBuffer;
                        }
                    });
                });
    }

    /**
     * Creates a new {@link BlobEncryptionPolicy} with given keyWrapper.
     *
     * @param keyWrapper
     *          A {@link IKey} to set.
     *
     * @return {@link BlobEncryptionPolicy}
     */
    public BlobEncryptionPolicy withKeyWrapper(IKey keyWrapper) {
        return new BlobEncryptionPolicy(keyWrapper, this.keyResolver, this.requireEncryption);
    }

    /**
     * Creates a new {@link BlobEncryptionPolicy} with given keyResolver.
     *
     * @param keyResolver
     *          A {@link IKeyResolver} to set.
     *
     * @return {@link BlobEncryptionPolicy}
     */
    public BlobEncryptionPolicy withKeyResolver(IKeyResolver keyResolver) {
        return new BlobEncryptionPolicy(this.keyWrapper, keyResolver, this.requireEncryption);
    }

    /**
     * Creates a new {@link BlobEncryptionPolicy} with given requireEncryption.
     *
     * @param requireEncryption
     *          boolean
     *
     * @return {@link BlobEncryptionPolicy}
     */
    public BlobEncryptionPolicy withRequireEncryption(boolean requireEncryption) {
        return new BlobEncryptionPolicy(this.keyWrapper, this.keyResolver, requireEncryption);
    }

    /**
     * Gets and validates {@link EncryptionData} from a Blob's metadata
     *
     * @param metadata
     *          {@code Map} of String -> String
     *
     * @return {@link EncryptionData}
     */
    private EncryptionData getAndValidateEncryptionData(Map<String, String> metadata) {
        Utility.assertNotNull("metadata", metadata);
        String encryptedDataString =  metadata.get(Constants.ENCRYPTION_DATA_KEY);
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            EncryptionData encryptionData = objectMapper.readValue(encryptedDataString, EncryptionData.class);

            if(encryptionData == null) {
                if(this.requireEncryption) {
                    throw new IllegalArgumentException(
                            "Encryption data does not exist. If you do not want to decrypt the data, please do not set the require encryption flag to true");
                }
                else {
                    return null;
                }
            }

            Utility.assertNotNull("contentEncryptionIV", encryptionData.contentEncryptionIV());
            Utility.assertNotNull("encryptedKey", encryptionData.wrappedContentKey().encryptedKey());

            // Throw if the encryption protocol on the message doesn't match the version that this client library understands
            // and is able to decrypt.
            if(!Constants.ENCRYPTION_PROTOCOL_V1.equals(encryptionData.encryptionAgent().protocol())) {
                throw new IllegalArgumentException(String.format(Locale.ROOT,
                        "Invalid Encryption Agent. This version of the client library does not understand the Encryption Agent set on the queue message: %s",
                        encryptionData.encryptionAgent()));
            }
            return encryptionData;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the key encryption key for blob.  First tries to get key encryption key from KeyResolver, then
     * falls back to IKey stored on this EncryptionPolicy.
     *
     * @param encryptionData
     *          A {@link EncryptionData}
     *
     * @return Key encryption key as a byte array
     */
    private Single<byte[]> getKeyEncryptionKey(EncryptionData encryptionData) {
        // 1. Invoke the key resolver if specified to get the key. If the resolver is specified but does not have a
        // mapping for the key id, an error should be thrown. This is important for key rotation scenario.
        // 2. If resolver is not specified but a key is specified, match the key id on the key and and use it.

        Single<IKey> keySingle;

        if(this.keyResolver != null) {
            keySingle = Single.fromFuture(this.keyResolver.resolveKeyAsync(encryptionData.wrappedContentKey().keyId()))
                    .onErrorResumeNext(e -> {
                        if(e instanceof NullPointerException) {

                            // keyResolver returns null if it cannot find the key, but RX doesn't allow null values
                            // passing through workflows, so we propagate this case with an IllegalArguementException
                            return Single.error(new IllegalArgumentException("KeyResolver could not resolve Key"));
                        }
                        else {
                            return Single.error(e);
                        }
                    });
        }
        else {
            if(encryptionData.wrappedContentKey().keyId().equals(this.keyWrapper.getKid())) {
                keySingle = Single.just(this.keyWrapper);
            }
            else {
                throw new IllegalArgumentException("Key mismatch. The key id stored on the service does not match the specified key.");
            }
        }

        return keySingle.flatMap(keyEncryptionKey ->
                Single.fromFuture(this.keyWrapper.unwrapKeyAsync(
                        encryptionData.wrappedContentKey().encryptedKey(),
                        encryptionData.wrappedContentKey().algorithm()
                )));
    }

    /**
     * Creates a {@link Cipher} using given content encryption key, encryption data, iv, and padding.
     *
     * @param contentEncryptionKey
     *          The content encryption key, used to decrypt the contents of the blob.
     * @param encryptionData
     *          {@link EncryptionData}
     * @param iv
     *          IV used to initialize the Cipher.  If IV is null, encryptionData
     * @param padding
     *          If cipher should use padding.  Padding is necessary to decrypt all the way to end of a blob.
     *          Otherwise, don't use padding.
     *
     * @return {@link Cipher}
     *
     * @throws InvalidKeyException
     */
    private Cipher getCipher(byte[] contentEncryptionKey, EncryptionData encryptionData, byte[] iv, boolean padding)
            throws InvalidKeyException {
        try {
            switch (encryptionData.encryptionAgent().algorithm()) {
                case AES_CBC_256:
                    Cipher cipher;
                    if(padding) {
                        cipher = Cipher.getInstance(Constants.AES_CBC_PKCS5PADDING);
                    }
                    else {
                        cipher = Cipher.getInstance(Constants.AES_CBC_NO_PADDING);
                    }
                    IvParameterSpec ivParameterSpec = new IvParameterSpec(iv);
                    SecretKey keySpec = new SecretKeySpec(contentEncryptionKey, 0, contentEncryptionKey.length,
                            Constants.AES);
                    cipher.init(Cipher.DECRYPT_MODE, keySpec, ivParameterSpec);
                    return cipher;
                default:
                    throw new IllegalArgumentException(
                            "Invalid Encryption Algorithm found on the resource. This version of the client library does not support the specified encryption algorithm.");
            }
        }
        catch(NoSuchPaddingException | NoSuchAlgorithmException | InvalidAlgorithmParameterException e) {
            throw new RuntimeException(e);
        }
    }
}
