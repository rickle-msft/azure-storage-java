package com.microsoft.azure.storage.file;

import com.microsoft.azure.storage.file.models.*;
import com.microsoft.rest.v2.Context;
import com.microsoft.rest.v2.http.HttpPipeline;
import io.reactivex.Flowable;
import io.reactivex.Single;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Locale;

import static com.microsoft.azure.storage.file.Utility.addErrorWrappingToSingle;

/**
 * Represents a URL to a file. It may be obtained by direct construction or via the create method on a {@link DirectoryURL} object.
 * This class does not hold any state about a particular file but is instead a convenient way of sending off appropriate requests
 * to the resource on the service. Please refer to the
 * <a href="https://docs.microsoft.com/en-us/azure/storage/files/storage-files-introduction">Azure Docs</a> for more information.
 */
public final class FileURL extends StorageURL {

    public static final int FILE_MAX_RANGE_LIMIT = 4 * Constants.MB;
    /**
     * Creates a {@code FileURL} object pointing to the account specified by the URL and using the provided
     * pipeline to make HTTP requests.
     *
     * @param url
     *         A {@code URL} to an Azure Storage Share.
     * @param pipeline
     *         A {@code HttpPipeline} which configures the behavior of HTTP exchanges. Please refer to the createPipeline
     *         method on {@link StorageURL} for more information.
     */
    public FileURL(URL url, HttpPipeline pipeline) {
        super(url, pipeline);
    }

    /**
     * Creates a new {@link FileURL} with the given pipeline.
     *
     * @param pipeline
     *         An {@link HttpPipeline} object to set.
     *
     * @return A {@link FileURL} object with the given pipeline.
     */
    public FileURL withPipeline(HttpPipeline pipeline) {
        try {
            return new FileURL(new URL(this.storageClient.url()), pipeline);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a new {@link FileURL} with the given pipeline.
     *
     * @param snapshot
     *         Specifies snapShot timestamp which shareURL object should point to.
     *
     * @return A {@link FileURL} object with the given pipeline.
     */
    public FileURL withSnapshot(String snapshot) {
        try {
            FileURLParts fileParts = URLParser.parse(this.toURL());
            fileParts.withShareSnapshot(snapshot);
            return new FileURL(fileParts.toURL(), this.storageClient.httpPipeline());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a new file or replaces a file. Note that this method only initializes the file. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/create-file">Azure Docs</a>.
     *
     * @param sizeInBytes
     *         Specifies the maximum size (in bytes) for the file, up to 1 TB
     * @param fileHTTPHeaders
     *         {@Link FileHTTPHeaders}
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
    public Single<FileCreateResponse> create(long sizeInBytes, FileHTTPHeaders fileHTTPHeaders, Metadata metadata, Context context) {
        context = context == null ? Context.NONE : context;
        metadata = metadata == null ? Metadata.NONE : metadata;

        return addErrorWrappingToSingle(this.storageClient.generatedFiles()
                .createWithRestResponseAsync(context, sizeInBytes, null, metadata, fileHTTPHeaders));
    }

    /**
     * Copies the data at the source URL to a file. For more information, see the
     * <a href="https://docs.microsoft.com/rest/api/storageservices/copy-file">Azure Docs</a>.
     *
     * @param copySource
     *         Specifies the URL of the source file or blob, up to 2 KB in length.
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
    public Single<FileStartCopyResponse> startCopy(URL copySource, Metadata metadata, Context context) {
        context = context == null ? Context.NONE : context;
        metadata = metadata == null ? Metadata.NONE : metadata;

        return addErrorWrappingToSingle(this.storageClient.generatedFiles()
                .startCopyWithRestResponseAsync(context, copySource.toString(), null, metadata));
    }

    /**
     * Stops a pending copy that was previously started and leaves a destination file with 0 length
     * and metadata. For more information, see the
     * <a href="https://docs.microsoft.com/rest/api/storageservices/abort-copy-file">Azure Docs</a>.
     *
     * @param copyId
     *         Specifies the copy identifier provided in the x-ms-copy-id header of the original Copy File operation
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<FileAbortCopyResponse> abortCopy(String copyId, Context context) {
        context = context == null ? Context.NONE : context;

        return addErrorWrappingToSingle(this.storageClient.generatedFiles()
                .abortCopyWithRestResponseAsync(context, copyId, null));
    }

    /**
     * Downloads count bytes of data from the start offset. If count is CountToEnd (0), then data is read from specified
     * offset to the end. For more information, see the
     * <a href="https://docs.microsoft.com/rest/api/storageservices/get-file">Azure Docs</a>.
     *
     * @param range
     *         {@Link FileRange}
     * @param rangeGetContentMD5
     *         Whether the contentMD5 for the specified file range should be returned.
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<DownloadResponse> download(FileRange range, boolean rangeGetContentMD5, Context context) {
        // If the rangeGetContentMD5 is passed as false, then set x-ms-range-get-content-md5 as null in the request.
        Boolean getMD5 = rangeGetContentMD5 ? rangeGetContentMD5 : null;
        // If the range is null, then pass empty string for x-ms-range
        String rangeString = range == null ? "" : range.toString();
        HTTPGetterInfo info = new HTTPGetterInfo();
        if(range != null) {
            info.withOffset(range.offset()).withCount(range.count());
        }

        return addErrorWrappingToSingle(this.storageClient.generatedFiles()
                .downloadWithRestResponseAsync(context, null, rangeString, getMD5))
                .map(response -> {
                    if (info.eTag() == null) {
                        // If there wasn't an etag originally specified, lock on the one returned.
                        info.withETag(response.headers().eTag());
                    }else{
                        // If there was an etag specified, compare the etag in the response against the locked etag.
                        if (!info.eTag().equals(response.headers().eTag())) {
                            String errorMessage = String.format(Locale.ROOT, "The response eTag %s did not match the initial eTag %s", info.eTag(), response.headers().eTag());
                            throw new IllegalStateException(errorMessage);
                        }
                    }
                    return new DownloadResponse(response, info,
                            // In the event of a stream failure, make a new request to pick up where we left off.
                            newInfo ->
                                    this.download(new FileRange().withOffset(newInfo.offset())
                                                    .withCount(newInfo.count()),
                                                         false,
                                            context == null ? Context.NONE : context));
                });
    }

    /**
     * Removes the file from the storage account immediately. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/delete-file2">Azure Docs</a>.
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
    public Single<FileDeleteResponse> delete(Context context) {
        context = context == null ? Context.NONE : context;

        return addErrorWrappingToSingle(this.storageClient.generatedFiles()
                .deleteWithRestResponseAsync(context, null));
    }

    /**
     * Returns the file's metadata and properties. For more information, see the
     * <a href="https://docs.microsoft.com/rest/api/storageservices/get-file-properties">Azure Docs</a>.
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
    public Single<FileGetPropertiesResponse> getProperties(Context context) {
        context = context == null ? Context.NONE : context;

        return addErrorWrappingToSingle(this.storageClient.generatedFiles()
                .getPropertiesWithRestResponseAsync(context, null, null));
    }

    /**
     * Sets file's system properties. For more information, see the
     * <a href="https://docs.microsoft.com/rest/api/storageservices/set-file-properties">Azure Docs</a>.
     *
     * @param fileHTTPHeaders
     *         {@Link FileHTTPHeaders}
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<FileSetHTTPHeadersResponse> setHTTPHeaders(FileHTTPHeaders fileHTTPHeaders, Context context) {
        context = context == null ? Context.NONE : context;
        return addErrorWrappingToSingle(this.storageClient.generatedFiles()
                .setHTTPHeadersWithRestResponseAsync(context, null, null, fileHTTPHeaders));
    }

    /**
     * SetMetadata sets a file's metadata. For more information, see the
     * <a href="https://docs.microsoft.com/rest/api/storageservices/set-file-metadata">Azure Docs</a>.
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
    public Single<FileSetMetadataResponse> setMetadata(Metadata metadata, Context context) {
        context = context == null ? Context.NONE : context;
        metadata = metadata == null ? Metadata.NONE : metadata;

        return addErrorWrappingToSingle(this.storageClient.generatedFiles()
                .setMetadataWithRestResponseAsync(context, null, metadata));
    }

    /**
     * Resize the file to the specified size. For more information, see the
     * <a href="https://docs.microsoft.com/rest/api/storageservices/set-file-properties">Azure Docs</a>.
     *
     * @param length
     *         Specifies the new size of the file. If the specified byte value is less than the current size
     *         of the file, then all ranges above the specified byte value are cleared.
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<FileSetHTTPHeadersResponse> resize(long length, Context context) {
        context = context == null ? Context.NONE : context;

        return addErrorWrappingToSingle(this.storageClient.generatedFiles()
                .setHTTPHeadersWithRestResponseAsync(context, null, length,null));
    }

    /**
     * Writes bytes to a file. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/put-range">Azure Docs</a>.
     *
     * @param range
     *          {@link FileRange}
     * @param data
     *         Specifies the intitial data to be written to the file.
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<FileUploadRangeResponse> uploadRange(FileRange range, Flowable<ByteBuffer> data, Context context) {
        context = context == null ? Context.NONE : context;
        range = range == null ? FileRange.DEFAULT : range;

        return addErrorWrappingToSingle(this.storageClient.generatedFiles()
                .uploadRangeWithRestResponseAsync(context, range.toString(), FileRangeWriteType.UPDATE,
                        range.count(), data, null, null));
    }

    /**
     * Clears the specified range and releases the space used in storage for that range. For more information,
     * see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/put-range">Azure Docs</a>.
     * @param range
     *          {@link FileRange}
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<FileUploadRangeResponse> clearRange(FileRange range, Context context) {
        context = context == null ? Context.NONE : context;
        range = range == null ? FileRange.DEFAULT : range;

        return addErrorWrappingToSingle(this.storageClient.generatedFiles()
                .uploadRangeWithRestResponseAsync(context, range.toString(), FileRangeWriteType.CLEAR,
                        0, null, null, null));
    }

    /**
     * Returns the list of valid ranges for a file. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/list-ranges">Azure Docs</a>.
     * @param range
     *          {@link FileRange}
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<FileGetRangeListResponse> getRangeList(FileRange range, Context context) {
        context = context == null ? Context.NONE : context;
        range = range == null ? FileRange.DEFAULT : range;

        return addErrorWrappingToSingle(this.storageClient.generatedFiles()
                .getRangeListWithRestResponseAsync(context, null, null, range.toString()));
    }
}
