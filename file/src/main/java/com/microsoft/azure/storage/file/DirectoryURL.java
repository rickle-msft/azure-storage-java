package com.microsoft.azure.storage.file;

import com.microsoft.azure.storage.file.models.*;
import com.microsoft.rest.v2.Context;
import com.microsoft.rest.v2.http.HttpPipeline;
import io.reactivex.Single;

import java.net.MalformedURLException;
import java.net.URL;

import static com.microsoft.azure.storage.file.Utility.addErrorWrappingToSingle;

public final class DirectoryURL extends StorageURL {

    /**
     * Creates a {@code DirectoryURL} object pointing to the account specified by the URL and using the provided
     * pipeline to make HTTP requests.
     *
     * @param url
     *         A {@code URL} to an Azure Storage Share.
     * @param pipeline
     *         A {@code HttpPipeline} which configures the behavior of HTTP exchanges. Please refer to the createPipeline
     *         method on {@link StorageURL} for more information.
     */
    public DirectoryURL(URL url, HttpPipeline pipeline) {
        super(url, pipeline);
    }

    /**
     * Creates a new {@link DirectoryURL} with the given pipeline.
     *
     * @param pipeline
     *         An {@link HttpPipeline} object to set.
     *
     * @return A {@link DirectoryURL} object with the given pipeline.
     */
    public DirectoryURL withPipeline(HttpPipeline pipeline) {
        try {
            return new DirectoryURL(new URL(this.storageClient.url()), pipeline);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a new {@link FileURL} object by concatenating the file name to the end of
     * DirectoryURL's URL. The new FileURL uses the same request policy pipeline as the DirectoryURL.
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
     * Creates a new {@link DirectoryURL} object by concatenating the directory name to the end of
     * DirectoryURL's URL. The new DirectoryURL uses the same request policy pipeline as the DirectoryURL.
     * To change the pipeline, create the DirectoryURL and then call its WithPipeline method passing in the
     * desired pipeline object. Or, call this package's NewDirectoryURL instead of calling this object's
     * createDirectoryURL method.
     *
     * @param directoryName
     *         A {@code String} representing the name of the directory.
     *
     * @return A new {@link DirectoryURL} object which references the directory with the specified name in this directory.
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
     * Creates a new directory within a storage account. For more information, see the
     * <a href="https://docs.microsoft.com/rest/api/storageservices/create-directory">Azure Docs</a>.
     *
     * @param metadata
     *         {@Link Metadata}
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<DirectoryCreateResponse> create(Metadata metadata, Context context) {
        context = context == null ? Context.NONE : context;
        metadata = metadata == null ? Metadata.NONE : metadata;

        return addErrorWrappingToSingle(this.storageClient.generatedDirectorys()
                .createWithRestResponseAsync(context, null, metadata));
    }

    /**
     * Removes the specified empty directory. Note that the directory must be empty
     * before it can be deleted. For more information, see the
     * <a href="https://docs.microsoft.com/rest/api/storageservices/delete-directory">Azure Docs</a>.
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
    public Single<DirectoryDeleteResponse> delete(Context context) {
        context = context == null ? Context.NONE : context;

        return addErrorWrappingToSingle(this.storageClient.generatedDirectorys()
                .deleteWithRestResponseAsync(context, null));
    }

    /**
     * Returns the directory's metadata and system properties. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/get-directory-properties">Azure Docs</a>.
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
    public Single<DirectoryGetPropertiesResponse> getProperties(Context context) {
        context = context == null ? Context.NONE : context;

        return addErrorWrappingToSingle(this.storageClient.generatedDirectorys()
                .getPropertiesWithRestResponseAsync(context, null, null));
    }

    /**
     * Sets the directory's metadata. For more information, see the
     * <a href="https://docs.microsoft.com/rest/api/storageservices/set-directory-metadata">Azure Docs</a>.
     *
     * @param metadata
     *         {@Link Metadata}
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<DirectorySetMetadataResponse> setMetadata(Metadata metadata, Context context) {
        context = context == null ? Context.NONE : context;
        metadata = metadata == null ? Metadata.NONE : metadata;

        return addErrorWrappingToSingle(this.storageClient.generatedDirectorys()
                .setMetadataWithRestResponseAsync(context, null, metadata));
    }

    /**
     * Returns a single segment of files and directories starting from the specified Marker.
     * For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/list-directories-and-files">Azure Docs</a>.
     *
     * @param prefix
     *         Specifies the prefix to filter the results to return directories or files whose names begin
     *         with the specified prefix.
     * @param marker
     *         Identifies the portion of the list to be returned with the next list operation. This value
     *         is returned in the response of a previous list operation as the
     *         DirectoryListFilesAndDirectoriesSegmentResponse.body().nextMarker(). Set to null to list
     *         the first segment.
     * @param maxResults
     *         Specifies the maximum number of directories or file to return, including all prefix elements.
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<DirectoryListFilesAndDirectoriesSegmentResponse> listFilesAndDirectoriesSegment(String prefix,
            String marker, Integer maxResults, Context context) {
        context = context == null ? Context.NONE : context;

        return addErrorWrappingToSingle(this.storageClient.generatedDirectorys()
                .listFilesAndDirectoriesSegmentWithRestResponseAsync(context, prefix, null, marker, maxResults, null));
    }
}
