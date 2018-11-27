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

package com.microsoft.azure.storage.file;

import com.microsoft.azure.storage.file.models.FileDownloadHeaders;
import com.microsoft.azure.storage.file.models.StorageErrorException;
import com.microsoft.rest.v2.util.FlowableUtil;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.Single;
import javafx.util.Pair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.Arrays;
import java.util.List;

import static java.lang.StrictMath.toIntExact;

/**
 * This class contains a collection of methods (and structures associated with those methods) which perform higher-level
 * operations. Whereas operations on the URL types guarantee a single REST request and make no assumptions on desired
 * behavior, these methods will often compose several requests to provide a convenient way of performing more complex
 * operations. Further, we will make our own assumptions and optimizations for common cases that may not be ideal for
 * rarer cases.
 */
public final class TransferManager {

    /**
     * Uploads the contents of a file to a azure file in parallel, breaking it into range-length size ranges if necessary.
     *
     * @param file
     *         The file to upload.
     * @param fileURL
     *         Points to the file to which the data should be uploaded.
     * @param rangeLength
     *         If the data must be broken up into ranges, this value determines what size those ranges will be.
     * @param options
     *         {@link TransferManagerUploadToFileOptions}
     * @return Emits the successful response.
     */
    public static Single<CommonRestResponse> uploadRangesToFile(
            final AsynchronousFileChannel file, final FileURL fileURL, final int rangeLength,
            final TransferManagerUploadToFileOptions options) throws IOException {
        Utility.assertNotNull("file", file);
        Utility.assertNotNull("fileURL", fileURL);
        Utility.assertInBounds("rangeLength", rangeLength, 1, FileURL.FILE_MAX_RANGE_LIMIT);
        TransferManagerUploadToFileOptions optionsReal = options == null ? TransferManagerUploadToFileOptions.DEFAULT : options;

        // Calculate the number of ranges.
        long numRanges = calculateNumRanges(file.size(), rangeLength);
        // Create empty file.
        if (numRanges == 0) {
            return fileURL.create(file.size(), optionsReal.httpHeaders(), optionsReal.metadata(), null)
                    .map(CommonRestResponse::createFileResponse);
        }

        return fileURL.create(file.size(), optionsReal.httpHeaders(), optionsReal.metadata(), null)
                .flatMap(response ->
                        /*
                        For each range, make a call to uploadRange as follows.
                        */
                        Observable.rangeLong(0, numRanges)
                        .flatMap(i -> {
                            // Calculate whether we need a full range or something smaller because we are at the end.
                            int count = (int)Math.min((long)rangeLength, (file.size() - (i * (long)rangeLength)));
                            Flowable<ByteBuffer> data = FlowableUtil.readFile(file, i * (long)rangeLength, count);
                            FileRange range = new FileRange().withOffset(i * rangeLength).withCount((long) count);
                            // Make the upload range call
                            return fileURL.uploadRange(range, data, null).toObservable();
                        }, optionsReal.parallelism())
                        // All the headers will be the same, so we just pick the last one.
                        .lastOrError())
                        .map(CommonRestResponse::uploadFileRangeResponse);
    }

    private static long calculateNumRanges(long dataSize, long blockLength) {
        // Can successfully cast to an int because MaxBlockSize is an int, which this expression must be less than.
        long numRanges = dataSize / blockLength;
        // Include an extra block for trailing data.
        if (dataSize % blockLength != 0) {
            numRanges++;
        }
        return numRanges;
    }

    /**
     * Downloads a file directly into a file, splitting the download into ranges and parallelizing as necessary.
     * @param file
     *         The destination file to which the file will be written.
     * @param fileURL
     *         The URL to the file to download.
     * @param range
     *         {@link FileRange}
     * @param options
     *         {@link TransferManagerDownloadFromFileOptions}
     * @return A {@code Completable} that will signal when the download is complete.
     */
    public static Single<FileDownloadHeaders> downloadToFile(AsynchronousFileChannel file, FileURL fileURL,
            FileRange range, TransferManagerDownloadFromFileOptions options) {
        FileRange r = range == null ? FileRange.DEFAULT : range;
        TransferManagerDownloadFromFileOptions o = options == null ? TransferManagerDownloadFromFileOptions.DEFAULT : options;
        Utility.assertNotNull("fileURL", fileURL);
        Utility.assertNotNull("file", file);

        // Get the size of the data and etag if not specified by the user.
        Single<List<Object>> setupSingle = getSetupSingle(fileURL, r, o);

        return setupSingle.flatMap(setupPair -> {
            Long dataSize = (Long) setupPair.get(0);
            String eTag = (String) setupPair.get(1);
            long numRanges = calculateNumRanges(dataSize, o.rangeLength());

            // In case it is an empty file, this ensures we still actually perform a download operation.
            numRanges = numRanges == 0 ? 1 : numRanges;

            return Observable.rangeLong(0, numRanges)
                    .flatMap(i -> {
                        // Calculate whether we need a full rangeLength or something smaller because we are at the end.
                        long chunkSizeActual = Math.min(o.rangeLength(),
                                dataSize - (i * o.rangeLength()));
                        FileRange rangeLength = new FileRange().withOffset(r.offset() + (i * o.rangeLength()))
                                .withCount(chunkSizeActual);
                        // Make the download call.
                        return fileURL.download(rangeLength,false, null)
                                // Extract the body.
                                .flatMapObservable(response -> {
                                    /*
                                     If the eTag in the Info doesn't matches the eTag of the downloaded response,
                                     it means that file has been modified and so the download should fail.
                                     */
                                    if (!response.headers().eTag().equals(eTag)){
                                        throw new IllegalStateException("Initial eTag from the file properties doesn't matches the eTag of the downloaded response", null);
                                    }
                                    // Write to the file.
                                    return FlowableUtil.writeFile(response.body(o.reliableDownloadOptionsPerRange()), file,
                                            i * o.rangeLength())
                                            /*
                                            Satisfy the return type. Observable required for flatmap to accept
                                            maxConcurrency. We want to eventually give the user back the headers.
                                             */
                                            .andThen(Single.just(response.headers()))
                                            .toObservable();
                                });
                    }, o.parallelism())
                    // All the headers will be the same, so we just pick the last one.
                    .lastOrError();
        });
    }

    private static Single<List<Object>> getSetupSingle(FileURL fileURL, FileRange r,
            TransferManagerDownloadFromFileOptions o) {
        /*
        Construct a Single which will emit the total count of bytes to be downloaded and retrieve an etag to lock on to
        if one was not specified. We use a single for this because we may have to make a REST call to get the length to
        calculate the count and we need to maintain asynchronicity.
         */
        if (r.count() == null || o.eTag() == null) {
            return fileURL.getProperties(null)
                    .map(response -> {

                        String eTag = o.eTag() == null ? response.headers().eTag() : o.eTag();
                        long newCount;
                        /*
                        If the user either didn't specify a count or they specified a count greater than the size of the
                        remaining data, take the size of the remaining data. This is to prevent the case where the count
                        is much much larger than the size of the file and we could try to download at an invalid offset.
                         */
                        if (r.count() == null || r.count() > response.headers().contentLength() - r.offset()) {
                            newCount = response.headers().contentLength() - r.offset();
                        } else {
                            newCount = r.count();
                        }
                        return Arrays.asList(newCount, eTag);
                    });
        } else {
            return Single.just(Arrays.asList(r.count(), o.eTag()));
        }
    }

}
