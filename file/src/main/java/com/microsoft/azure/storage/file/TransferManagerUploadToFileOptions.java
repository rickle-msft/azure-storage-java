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

import com.microsoft.azure.storage.file.models.FileHTTPHeaders;

/**
 * Configures the parallel upload behavior for methods on the {@code TransferManager}.
 */
public class TransferManagerUploadToFileOptions {

    /**
     * An object which represents the default parallel upload options.
     */
    public static final TransferManagerUploadToFileOptions DEFAULT = new TransferManagerUploadToFileOptions(null, null,
            null, null);

    private final IProgressReceiver progressReceiver;

    private final FileHTTPHeaders httpHeaders;

    private final Metadata metadata;

    private final int parallelism;

    public IProgressReceiver progressReceiver() {
        return progressReceiver;
    }

    public FileHTTPHeaders httpHeaders() {
        return httpHeaders;
    }

    public Metadata metadata() {
        return metadata;
    }

    public int parallelism() {
        return parallelism;
    }

    /**
     * Creates a new object that configures the parallel upload behavior. Null may be passed to accept the default
     * behavior.
     *
     * @param progressReceiver
     *         An object that implements the {@link IProgressReceiver} interface which will be invoked periodically
     *         as bytes are sent in a UploadRange call to the FileURL. May be null if no progress reports are
     *         desired.
     * @param httpHeaders
     *         Most often used when creating a file or setting its properties, this class contains fields for typical HTTP
     *         properties, which, if specified, will be attached to the target file. Null may be passed to any API which takes this
     *         type to indicate that no properties should be set.
     * @param metadata
     *         {@link Metadata}
     * @param parallelism
     *         A {@code int} that indicates the maximum number of ranges to upload in parallel. Must be greater than
     *         0. May be null to accept default behavior.
     */
    public TransferManagerUploadToFileOptions(IProgressReceiver progressReceiver, FileHTTPHeaders httpHeaders,
            Metadata metadata, Integer parallelism) {
        if (parallelism == null) {
            this.parallelism = Constants.TRANSFER_MANAGER_DEFAULT_PARALLELISM;
        } else if (parallelism <= 0) {
            throw new IllegalArgumentException("Parallelism must be > 0");
        } else {
            this.parallelism = parallelism;
        }

        this.progressReceiver = progressReceiver;
        this.httpHeaders = httpHeaders;
        this.metadata = metadata;
    }
}
