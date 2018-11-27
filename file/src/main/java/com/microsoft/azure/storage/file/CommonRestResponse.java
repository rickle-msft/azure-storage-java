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

import com.microsoft.azure.storage.file.models.FileCreateResponse;
import com.microsoft.azure.storage.file.models.FileUploadRangeResponse;
import com.microsoft.rest.v2.RestResponse;

import java.time.OffsetDateTime;

/**
 * A generic wrapper for any type of file REST API response. Used and returned by methods in the {@link TransferManager}
 * class. The methods there return this type because they represent composite operations which may conclude with any of
 * several possible REST calls depending on the data provided.
 */
public final class CommonRestResponse {

    private FileCreateResponse createFileResponse;
    private FileUploadRangeResponse uploadRangeResponse;

    static CommonRestResponse createFileResponse(FileCreateResponse response) {
        CommonRestResponse commonRestResponse = new CommonRestResponse();
        commonRestResponse.createFileResponse = response;
        return commonRestResponse;
    }

    static CommonRestResponse uploadFileRangeResponse(FileUploadRangeResponse response) {
        CommonRestResponse commonRestResponse = new CommonRestResponse();
        commonRestResponse.uploadRangeResponse = response;
        return commonRestResponse;
    }

    private CommonRestResponse() {
        uploadRangeResponse = null;
        createFileResponse = null;
    }

    /**
     * @return
     *      The status code for the response
     */
    public int statusCode() {
        if (uploadRangeResponse != null) {
            return uploadRangeResponse.statusCode();
        }
        return createFileResponse.statusCode();
    }

    /**
     * @return
     *      An HTTP Etag for the file at the time of the request.
     */
    public String eTag() {
        if (uploadRangeResponse != null) {
            return uploadRangeResponse.headers().eTag();
        }
        return createFileResponse.headers().eTag();
    }

    /**
     * @return
     *      The time when the file was last modified.
     */
    public OffsetDateTime lastModified() {
        if (uploadRangeResponse != null) {
            return uploadRangeResponse.headers().lastModified();
        }
        return createFileResponse.headers().lastModified();
    }

    /**
     * @return
     *      The id of the service request for which this is the response.
     */
    public String requestId() {
        if (uploadRangeResponse != null) {
            return uploadRangeResponse.headers().requestId();
        }
        return createFileResponse.headers().requestId();
    }

    /**
     * @return
     *      The date of the response.
     */
    public OffsetDateTime date() {
        if (uploadRangeResponse != null) {
            return uploadRangeResponse.headers().date();
        }
        return createFileResponse.headers().date();
    }

    /**
     * @return
     *       The service version responding to the request.
     */
    public String version() {
        if (uploadRangeResponse != null) {
            return uploadRangeResponse.headers().version();
        }
        return createFileResponse.headers().version();
    }

    /**
     * @return
     *      The underlying response.
     */
    public RestResponse response() {
        if (uploadRangeResponse != null) {
            return uploadRangeResponse;
        }
        return createFileResponse;
    }

}
