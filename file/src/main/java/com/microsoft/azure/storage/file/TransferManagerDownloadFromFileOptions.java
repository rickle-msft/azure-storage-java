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

/**
 * Configures the parallel download behavior for methods on the {@code TransferManager}.
 */
public final class TransferManagerDownloadFromFileOptions {

    /**
     * The default download options.
     */
    public static final TransferManagerDownloadFromFileOptions DEFAULT =
            new TransferManagerDownloadFromFileOptions(null, null, null, null, null);

    private final long rangeLength;
    private final String eTag;
    private final IProgressReceiver progressReceiver;
    private final int parallelism;
    private final ReliableDownloadOptions reliableDownloadOptionsPerRange;

    public long rangeLength() {
        return rangeLength;
    }

    public IProgressReceiver progressReceiver() {
        return progressReceiver;
    }

    public int parallelism() {
        return parallelism;
    }

    public ReliableDownloadOptions reliableDownloadOptionsPerRange() {
        return reliableDownloadOptionsPerRange;
    }

    public String eTag(){
        return eTag;
    }

    /**
     * Returns an object that configures the parallel download behavior for methods on the {@code TransferManager}.
     *
     * @param rangeLength
     *         The size of the ranges into which large download operations will be broken into. Note that if the
     *         rangeLength is large, fewer but larger requests will be made as each REST request will download a
     *         single range in full. For larger range length, it may be helpful to configure the
     *         {@code reliableDownloadOptions} to allow more retries.
     * @param progressReceiver
     *         {@link IProgressReceiver}
     * @param parallelism
     *         A {@code int} that indicates the maximum number of ranges to download in parallel. Must be greater
     *         than 0. May be null to accept default behavior.
     * @param reliableDownloadOptions
     *         {@link ReliableDownloadOptions}
     */
    public TransferManagerDownloadFromFileOptions(Long rangeLength, String eTag, IProgressReceiver progressReceiver,
            Integer parallelism, ReliableDownloadOptions reliableDownloadOptions) {
        if (rangeLength != null) {
            Utility.assertInBounds("rangeLength", rangeLength, 1, FileURL.FILE_MAX_RANGE_LIMIT);
            this.rangeLength = rangeLength;
        } else {
            this.rangeLength = FileURL.FILE_MAX_RANGE_LIMIT;
        }

        if (parallelism != null) {
            Utility.assertInBounds("parallelism", parallelism, 1, Integer.MAX_VALUE);
            this.parallelism = parallelism;
        } else {
            this.parallelism = Constants.TRANSFER_MANAGER_DEFAULT_PARALLELISM;
        }
        this.eTag = eTag;
        this.progressReceiver = progressReceiver;
        this.reliableDownloadOptionsPerRange = reliableDownloadOptions == null ?
                new ReliableDownloadOptions() : reliableDownloadOptions;
    }
}
