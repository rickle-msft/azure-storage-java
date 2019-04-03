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

import static com.microsoft.azure.storage.blob.encryption.Constants.ENCRYPTION_BLOCK_SIZE;

/**
 * Represents a blob encryption policy that is used to perform envelope encryption/decryption of Azure storage blobs.
 * This class is immutable as it is a property on an EncryptedBlobURL, which needs to be thread safe.
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
     * Encrypts the given Flowable ByteBuffer.
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
                                        new EncryptionAgent(Constants.ENCRYPTION_PROTOCOL_V1,
                                                EncryptionAlgorithm.AES_CBC_256))
                                .withKeyWrappingMetadata(keyWrappingMetadata)
                                .withContentEncryptionIV(cipher.getIV())
                                .withWrappedContentKey(wrappedKey);

                        // Encrypt plain text with content encryption key
                        Flowable<ByteBuffer> encryptedTextFlowable = plainTextFlowable.map(plainTextBuffer -> {
                            int outputSize = cipher.getOutputSize(plainTextBuffer.remaining());

                            /*
                            This should be the only place we allocate memory in encryptBlob(). Although there is an
                            overload that can encrypt in place that would save allocations, we do not want to overwrite
                            customer's memory, so we must allocate our own memory. If memory usage becomes unreasonable,
                            we should implement pooling.
                             */
                            ByteBuffer encryptedTextBuffer = ByteBuffer.allocate(outputSize);

                            int encryptedBytes = cipher.update(plainTextBuffer, encryptedTextBuffer);
                            encryptedTextBuffer.position(0);
                            encryptedTextBuffer.limit(encryptedBytes);
                            return encryptedTextBuffer;
                        });

                        /*
                        Defer() ensures the contained code is not executed until the Flowable is subscribed to, in
                        other words, cipher.doFinal() will not be called until the plainTextFlowable has completed
                        and therefore all other data has been encrypted.
                         */
                        encryptedTextFlowable = Flowable.concat(encryptedTextFlowable, Flowable.defer(() ->
                                Flowable.just(ByteBuffer.wrap(cipher.doFinal()))));
                        return new EncryptedBlob(encryptionData, encryptedTextFlowable);
                    });
        }
        // These are hardcoded and guaranteed to work. There is no reason to propogate a checked exception.
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
     *          The encrypted Flowable of ByteBuffer to decrypt
     * @param encryptedBlobRange
     *          A {@link EncryptedBlobRange} indicating the range to decrypt
     * @param padding
     *          Boolean indicating if the padding mode should be set or not.
     *
     * @return A Flowable ByteBuffer that has been decrypted
     */
    Flowable<ByteBuffer> decryptBlob(Map<String, String> metadata, Flowable<ByteBuffer> encryptedFlowable,
                                            EncryptedBlobRange encryptedBlobRange, boolean padding) {
        if(this.keyWrapper == null && this.keyResolver == null) {
            throw new IllegalArgumentException("Key and KeyResolver cannot both be null");
        }

        EncryptionData encryptionData = getAndValidateEncryptionData(metadata);
        if(encryptionData == null) {
            if (this.requireEncryption) {
                throw new IllegalStateException("Require encryption is set to true but the blob is not encrypted.");
            }
            else {
                return encryptedFlowable;
            }
        }

        // The number of bytes we have processed so far, not including the Cipher buffer.
        AtomicLong processedBytes = new AtomicLong(0);

        return getKeyEncryptionKey(encryptionData)
                .flatMapPublisher(contentEncryptionKey -> {

                    /*
                    Calculate the IV.

                    If we are starting at the beginning, we can grab the IV from the encryptionData. Otherwise,
                    Rx makes it difficult to grab the first 16 bytes of data to pass as an IV to the cipher.
                    As a work around, we initialize the cipher with a garbage IV (empty byte array) and attempt to
                    decrypt the first 16 bytes (the actual IV for the relevant data). We throw away this "decrypted"
                    data. Now, though, because each block of 16 is used as the IV for the next, the original 16 bytes
                    of downloaded data are in position to be used as the IV for the data actually requested and we are
                    in the desired state.
                     */
                    byte[] IV = new byte[ENCRYPTION_BLOCK_SIZE];
                    /*
                    Adjusting the range by <= 16 means we only adjusted to align on an encryption block boundary
                    (padding will add 1-16 bytes as it will prefer to pad 16 bytes instead of 0) and therefore the key
                    is in the metadata.
                     */
                    if(encryptedBlobRange.offsetAdjustment() <= ENCRYPTION_BLOCK_SIZE) {
                        IV = encryptionData.contentEncryptionIV();
                    }

                    Cipher cipher = getCipher(contentEncryptionKey, encryptionData, IV, padding);

                    return encryptedFlowable.map(encryptedByteBuffer -> {
                        ByteBuffer plaintextByteBuffer;
                        int outputSize = cipher.getOutputSize(encryptedByteBuffer.remaining());

                        /*
                        If we could potentially decrypt more bytes than encryptedByteBuffer can hold, allocate more
                        room. Note that, because getOutputSize returns the size needed to store
                        max(updateOutputSize, finalizeOutputSize), this is likely to produce a ByteBuffer slightly
                        larger than what the real outputSize is. This is accounted for below.
                         */
                        if(outputSize > encryptedByteBuffer.limit()) {
                            plaintextByteBuffer = ByteBuffer.allocate(outputSize);
                        }
                        else {
                            /*
                            This is an ok optimization on download as the underlying buffer is not the customer's but
                            the protocol layer's, which does not expect to be able to reuse it.
                             */
                            plaintextByteBuffer = encryptedByteBuffer.duplicate();
                        }

                        int decryptedBytes;

                        // First, determine if we should update or finalize and fill the output buffer.

                        // We will have reached the end of the downloaded range. Finalize.
                        if (processedBytes.longValue() + outputSize >= encryptedBlobRange.adjustedDownloadCount()) {
                            decryptedBytes = cipher.doFinal(encryptedByteBuffer, plaintextByteBuffer);
                        }
                        // We will not have reached the end of the downloaded range. Update.
                        else {
                            decryptedBytes = cipher.update(encryptedByteBuffer, plaintextByteBuffer);
                        }
                        plaintextByteBuffer.position(0); // Reset the position after writing.

                        // Next, determine and set the position of the output buffer.

                        /*
                        The beginning of this ByteBuffer has not yet reached customer-requested data. i.e. it starts
                        somewhere in either the IV or the range adjustment to align on a block boundary. We should
                        advance the position so the customer does not read this data.
                         */
                        if (processedBytes.longValue() <= encryptedBlobRange.offsetAdjustment()) {
                            /*
                            Note that the cast is safe because of the bounds on offsetAdjustment (see encryptedBlobRange
                            for details), which here upper bounds processedBytes.
                            Note that we do not simply set the position to be offsetAdjustment because of the (unlikely)
                            case that some ByteBuffers were small enough to be entirely contained within the
                            offsetAdjustment, so when we do reach customer-requested data, advancing the position by
                            the whole offsetAdjustment would be too much.
                             */
                            int remainingAdjustment = encryptedBlobRange.offsetAdjustment() -
                                    (int)processedBytes.longValue();

                            /*
                            Setting the position past the limit will throw. This is in the case of very small
                            ByteBuffers that are entirely contained within the offsetAdjustment.
                             */
                            int newPosition = remainingAdjustment <= plaintextByteBuffer.limit() ?
                                    remainingAdjustment : plaintextByteBuffer.limit();

                            plaintextByteBuffer.position(newPosition);

                        }
                        /*
                        Else: The beginning of this ByteBuffer is somewhere after the offset adjustment. If it is in the
                        middle of customer requested data, let it be. If it starts in the end adjustment, this will
                        be trimmed effectively by setting the limit below.
                         */

                        // Finally, determine and set the limit of the output buffer.

                        long beginningOfEndAdjustment; // read: beginning of end-adjustment.
                        /*
                        The user intended to download the whole blob, so the only extra we downloaded was padding, which
                        is trimmed by the cipher automatically; there is effectively no beginning to the end-adjustment.
                         */
                        if (encryptedBlobRange.originalRange().count() == null) {
                            beginningOfEndAdjustment = Long.MAX_VALUE;
                        }
                        // Calculate the end of the user-requested data so we can trim anything after.
                        else {
                            beginningOfEndAdjustment = encryptedBlobRange.offsetAdjustment() +
                                    encryptedBlobRange.originalRange().count();
                        }

                        /*
                        The end of this ByteBuffer lies after customer requested data (in the end adjustment) and
                        should be trimmed.
                         */
                        if (decryptedBytes + processedBytes.longValue() > beginningOfEndAdjustment) {
                            long amountPastEnd // past the end of user-requested data.
                                    = decryptedBytes + processedBytes.longValue() - beginningOfEndAdjustment;
                            /*
                            Note that amountPastEnd can only be up to 16, so the cast is safe. We do not need to worry
                            about limit() throwing because we allocated at least enough space for decryptedBytes and
                            the newLimit will be less than that. In the case where this ByteBuffer starts after the
                            beginning of the endAdjustment, we don't want to send anything back, so we set limit to be
                            the same as position.
                             */
                            int newLimit = processedBytes.longValue() <= beginningOfEndAdjustment ?
                                    decryptedBytes - (int)amountPastEnd : plaintextByteBuffer.position();
                            plaintextByteBuffer.limit(newLimit);
                        }
                        /*
                        The end of this ByteBuffer is before the end adjustment and after the offset adjustment, so it
                        will lie somewhere in customer requested data. It is possible we allocated a ByteBuffer that is
                        slightly too large, so we set the limit equal to exactly the amount we decrypted to be safe.
                         */
                        else if (decryptedBytes + processedBytes.longValue() > encryptedBlobRange.offsetAdjustment()) {
                            plaintextByteBuffer.limit(decryptedBytes);
                        }
                        /*
                        Else: The end of this ByteBuffer will not reach the beginning of customer-requested data. This
                        has already been trimmed.
                         */

                        /*
                        Update processedBytes at the end so we preserve our original start value until we're finished
                        with calculations.
                         */
                        processedBytes.addAndGet(decryptedBytes);
                        return plaintextByteBuffer;
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
                            "Encryption data does not exist. If you do not want to decrypt the data, please do not " +
                                    "set the require encryption flag to true");
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
                        "Invalid Encryption Agent. This version of the client library does not understand the " +
                                "Encryption Agent set on the queue message: %s",
                        encryptionData.encryptionAgent()));
            }
            return encryptionData;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the key encryption key for blob. First tries to get key encryption key from KeyResolver, then
     * falls back to IKey stored on this EncryptionPolicy.
     *
     * @param encryptionData
     *          A {@link EncryptionData}
     *
     * @return Key encryption key as a byte array
     */
    private Single<byte[]> getKeyEncryptionKey(EncryptionData encryptionData) {
        /*
        1. Invoke the key resolver if specified to get the key. If the resolver is specified but does not have a
        mapping for the key id, an error should be thrown. This is important for key rotation scenario.
        2. If resolver is not specified but a key is specified, match the key id on the key and and use it.
        */

        Single<IKey> keySingle;

        if(this.keyResolver != null) {
            keySingle = Single.fromFuture(this.keyResolver.resolveKeyAsync(encryptionData.wrappedContentKey().keyId()))
                    .onErrorResumeNext(e -> {
                        if(e instanceof NullPointerException) {
                            /*
                            keyResolver returns null if it cannot find the key, but RX throws on null values
                            passing through workflows, so we propagate this case with an IllegalArgumentException
                             */
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
                throw new IllegalArgumentException("Key mismatch. The key id stored on the service does not match " +
                        "the specified key.");
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
     *          If cipher should use padding. Padding is necessary to decrypt all the way to end of a blob.
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
                            "Invalid Encryption Algorithm found on the resource. This version of the client library " +
                                    "does not support the specified encryption algorithm.");
            }
        }
        catch(NoSuchPaddingException | NoSuchAlgorithmException | InvalidAlgorithmParameterException e) {
            throw new RuntimeException(e);
        }
    }
}
