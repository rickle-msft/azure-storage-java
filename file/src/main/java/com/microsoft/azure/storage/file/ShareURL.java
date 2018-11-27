package com.microsoft.azure.storage.file;

import com.microsoft.azure.storage.file.models.*;
import com.microsoft.rest.v2.Context;
import com.microsoft.rest.v2.http.HttpPipeline;
import io.reactivex.Single;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static com.microsoft.azure.storage.file.Utility.addErrorWrappingToSingle;

/**
 * Represents a URL to a share. It may be obtained by direct construction or via the create method on a
 * {@link ServiceURL} object. This class does not hold any state about a particular file but is instead a convenient way
 * of sending off appropriate requests to the resource on the service. It may also be used to construct URLs to files.
 * Please refer to the
 * <a href=https://docs.microsoft.com/en-us/azure/storage/files/storage-files-introduction>Azure Docs</a>
 * for more information on shares.
 */
public final class ShareURL extends StorageURL {

    /**
     * Creates a {@code ShareURL} object pointing to the account specified by the URL and using the provided
     * pipeline to make HTTP requests.
     *
     * @param url
     *         A {@code URL} to an Azure Storage Share.
     * @param pipeline
     *         A {@code HttpPipeline} which configures the behavior of HTTP exchanges. Please refer to the createPipeline
     *         method on {@link StorageURL} for more information.
     */
    public ShareURL(URL url, HttpPipeline pipeline) {
        super(url, pipeline);
    }

    /**
     * Creates a new {@link ShareURL} with the given pipeline.
     *
     * @param pipeline
     *         An {@link HttpPipeline} object to set.
     *
     * @return A {@link ShareURL} object with the given pipeline.
     */
    public ShareURL withPipeline(HttpPipeline pipeline) {
        try {
            return new ShareURL(new URL(this.storageClient.url()), pipeline);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a new ShareURL object identical to the source but with the specified snapshot timestamp.
     *
     * @param shareSnapShot
     *         Specifies snapShot timestamp which shareURL object should point to.
     *
     * @return A {@link ShareURL} object with the given pipeline.
     */
    public ShareURL withSnapshot(String shareSnapShot) {
        try {
            FileURLParts fileParts = URLParser.parse(this.toURL());
            fileParts.withShareSnapshot(shareSnapShot);
            return new ShareURL(fileParts.toURL(), this.storageClient.httpPipeline());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a new {@link DirectoryURL} object by concatenating the directory name to the end of
     * ShareURL's URL. The new DirectoryURL uses the same request policy pipeline as the ShareURL.
     * To change the pipeline, create the DirectoryURL and then call its WithPipeline method passing in the
     * desired pipeline object. Or, call this package's NewDirectoryURL instead of calling this object's
     * createDirectoryURL method.
     *
     * @param directoryName
     *         A {@code String} representing the name of the directory.
     *
     * @return A new {@link DirectoryURL} object which references the directory with the specified name in this share.
     */
    public DirectoryURL createDirectoryURL(String directoryName) {
        try {
            return new DirectoryURL(StorageURL.appendToURLPath(new URL(this.storageClient.url()), directoryName),
                    this.storageClient.httpPipeline());
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a new {@link DirectoryURL} object using the ShareURL's URL. The new DirectoryURL uses the same
     * request policy pipeline as the ShareURL. To change the pipeline, create the DirectoryURL and then call
     * its WithPipeline method passing in the desired pipeline object. Or, call this package's NewDirectoryURL
     * instead of calling this object's createRootDirectoryURL method.
     *
     * @return A new {@link DirectoryURL} object which references the root directory in this share.
     */
    public DirectoryURL createRootDirectoryURL() {
        try {
            return new DirectoryURL(new URL(this.storageClient.url()),
                    this.storageClient.httpPipeline());
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a new {@link FileURL} object by concatenating the file name to the end of
     * ShareURL's URL. The new FileURL uses the same request policy pipeline as the ShareURL.
     * To change the pipeline, create the FileURL and then call its WithPipeline method passing in the
     * desired pipeline object. Or, call this package's NewFileURL instead of calling this object's
     * createFileURL method.
     *
     * @param fileName
     *         A {@code String} representing the name of the file.
     *
     * @return A new {@link FileURL} object which references the file with the specified name in this directory.
     */
    public FileURL createFileURL(String fileName) {
        try {
            return new FileURL(StorageURL.appendToURLPath(new URL(this.storageClient.url()), fileName),
                    this.storageClient.httpPipeline());
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a new share under the specified account. If the share with the same name already exists,
     * the operation fails. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/create-share">Azure Docs</a>.
     *
     * @return Emits the successful response.
     */
    public Single<ShareCreateResponse> create() {
        return create(null, null, null);
    }


    /**
     * Creates a new share under the specified account. If the share with the same name already exists,
     * the operation fails. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/create-share">Azure Docs</a>.
     *
     * @param metadata
     *         {@link Metadata}
     * @param quotaInGB
     *         Specifies the maximum size of the share, in gigabytes.
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<ShareCreateResponse> create(Metadata metadata, Integer quotaInGB, Context context) {
        context = context == null ? Context.NONE : context;
        metadata = metadata == null ? Metadata.NONE : metadata;

        return addErrorWrappingToSingle(this.storageClient.generatedShares()
                .createWithRestResponseAsync(context, null, metadata, quotaInGB));
    }

    /**
     * Creates a read-only snapshot of a share. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/snapshot-share">Azure Docs</a>
     *
     * @return Emits the successful response.
     */
    public Single<ShareCreateSnapshotResponse> createSnapshot() {
        return createSnapshot(null, null);
    }

    /**
     * Creates a read-only snapshot of a share. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/snapshot-share">Azure Docs</a>
     *
     * @param metadata
     *         A {@Link Metadata}
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */

    public Single<ShareCreateSnapshotResponse> createSnapshot(Metadata metadata, Context context) {
        context = context == null ? Context.NONE : context;
        metadata = metadata == null ? Metadata.NONE : metadata;

        return addErrorWrappingToSingle(this.storageClient.generatedShares()
                .createSnapshotWithRestResponseAsync(context, null, metadata));
    }

    /**
     * Marks the specified share or share snapshot for deletion. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/delete-share">Azure Docs</a>
     *
     * @return Emits the successful response.
     */
    public Single<ShareDeleteResponse> delete() {
        return delete(null, null);
    }

    /**
     * Marks the specified share or share snapshot for deletion. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/delete-share">Azure Docs</a>
     *
     * @param deleteSnapshotsOptionType
     *         Specifies {@Link DeleteSnapshotsOptionType} for snapshots if share has associated snapshots.
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<ShareDeleteResponse> delete(DeleteSnapshotsOptionType deleteSnapshotsOptionType, Context context) {
        context = context == null ? Context.NONE : context;

        return addErrorWrappingToSingle(this.storageClient.generatedShares()
                .deleteWithRestResponseAsync(context, null, null, deleteSnapshotsOptionType));
    }

    /**
     * Returns all user-defined metadata and system properties for the specified share or share snapshot.
     * The data returned does not include the share's list of files. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/get-share-properties">Azure Docs</a>
     *
     * @return Emits the successful response.
     */
    public Single<ShareGetPropertiesResponse> getProperties() {
        return getProperties(null);
    }

    /**
     * Returns all user-defined metadata and system properties for the specified share or share snapshot.
     * The data returned does not include the share's list of files. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/get-share-properties">Azure Docs</a>
     *
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<ShareGetPropertiesResponse> getProperties(Context context) {
        context = context == null ? Context.NONE : context;

        return addErrorWrappingToSingle(this.storageClient.generatedShares()
                .getPropertiesWithRestResponseAsync(context, null, null));
    }

    /**
     * Sets service-defined properties for the specified share. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/get-share-properties">Azure Docs</a>
     *
     * @param metadata
     *         A {@Link Metadata}
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<ShareSetMetadataResponse> setMetadata(Metadata metadata, Context context) {
        context = context == null ? Context.NONE : context;
        metadata = metadata == null ? Metadata.NONE : metadata;

        return addErrorWrappingToSingle(this.storageClient.generatedShares()
                .setMetadataWithRestResponseAsync(context, null, metadata));
    }

    /**
     * Sets service-defined properties for the specified share. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/get-share-properties">Azure Docs</a>
     *
     * @param quotaInGB
     *         Specifies the maximum size of the share in gigabytes, 0 means no quote and uses service's default value.
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<ShareSetQuotaResponse> setQuota(int quotaInGB, Context context) {
        context = context == null ? Context.NONE : context;

        return addErrorWrappingToSingle(this.storageClient.generatedShares()
                .setQuotaWithRestResponseAsync(context, null, quotaInGB));
    }

    /**
     * Retrieves information about stored access policies specified on the share. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/set-share-properties">Azure Docs</a>
     *
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<ShareGetAccessPolicyResponse> getAccessPolicy(Context context) {
        context = context == null ? Context.NONE : context;

        return addErrorWrappingToSingle(this.storageClient.generatedShares()
                .getAccessPolicyWithRestResponseAsync(context, null));
    }

    /**
     * Sets service-defined properties for the specified share. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/set-share-properties">Azure Docs</a>
     *
     * @param identifiers
     *         A list of {@link SignedIdentifier} objects that specify the permissions for the share. Please see
     *         <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/establishing-a-stored-access-policy">here</a>
     *         for more information. Passing null will clear all access policies.
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<ShareSetAccessPolicyResponse> setAccessPolicy(List<SignedIdentifier> identifiers, Context context) {
        context = context == null ? Context.NONE : context;

        /*
        We truncate to seconds because the service only supports nanoseconds or seconds, but doing an
        OffsetDateTime.now will only give back milliseconds (more precise fields are zeroed and not serialized). This
        allows for proper serialization with no real detriment to users as sub-second precision on active time for
        signed identifiers is not really necessary.
         */
        if (identifiers != null) {
            for (SignedIdentifier identifier : identifiers) {
                if (identifier.accessPolicy() != null && identifier.accessPolicy().start() != null) {
                    identifier.accessPolicy().withStart(
                            identifier.accessPolicy().start().truncatedTo(ChronoUnit.SECONDS));
                }
                if (identifier.accessPolicy() != null && identifier.accessPolicy().expiry() != null) {
                    identifier.accessPolicy().withExpiry(
                            identifier.accessPolicy().expiry().truncatedTo(ChronoUnit.SECONDS));
                }
            }
        }

        return addErrorWrappingToSingle(this.storageClient.generatedShares()
                .setAccessPolicyWithRestResponseAsync(context, identifiers, null));
    }

    /**
     * Retrieves statistics related to the specified share. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/get-share-stats">Azure Docs</a>
     *
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<ShareGetStatisticsResponse> getStatistics(Context context) {
        context = context == null ? Context.NONE : context;

        return addErrorWrappingToSingle(this.storageClient.generatedShares()
                .getStatisticsWithRestResponseAsync(context, null));
    }
}
