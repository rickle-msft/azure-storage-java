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

package com.microsoft.azure.storage.blob;

import com.microsoft.azure.storage.blob.models.BlobDownloadHeaders;
import com.microsoft.rest.v2.RestResponse;
import io.reactivex.Flowable;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * {@code DownloadResponse}
 */
public class DownloadResponse {
    protected final HTTPGetterInfo info;

    protected RestResponse<BlobDownloadHeaders, Flowable<ByteBuffer>> rawResponse;

    public DownloadResponse(RestResponse<BlobDownloadHeaders, Flowable<ByteBuffer>> response, HTTPGetterInfo info) {
        Utility.assertNotNull("info", info);
        Utility.assertNotNull("info.eTag", info.eTag());
        this.rawResponse = response;
        this.info = info;
    }

    /**
     * Returns the response body.
     *
     * @return A {@code Flowable} which emits the data as {@code ByteBuffer}s.
     */
    public Flowable<ByteBuffer> body(ReliableDownloadOptions options) {
        return this.rawResponse.body();
    }

    public int statusCode() {
        return this.rawResponse.statusCode();
    }

    public BlobDownloadHeaders headers() {
        return this.rawResponse.headers();
    }

    public Map<String, String> rawHeaders() {
        return this.rawResponse.rawHeaders();
    }

    public RestResponse<BlobDownloadHeaders, Flowable<ByteBuffer>> rawResponse() {
        return this.rawResponse;
    }

    public HTTPGetterInfo info() {
        return this.info;
    }

    public DownloadResponse withRestResponse(RestResponse<BlobDownloadHeaders, Flowable<ByteBuffer>> restResponse) {
        this.rawResponse = restResponse;
        return this;
    }
}