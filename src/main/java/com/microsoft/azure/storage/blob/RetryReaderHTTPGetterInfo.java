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

/**
 * HTTPGetterInfo is a passed to the getter function of a RetryReader to specify parameters needed for the GET
 * request.
 */
public class RetryReaderHTTPGetterInfo {
    /**
     * The start offset that should be used when creating the HTTP GET request's Range header. Defaults to 0.
     */
    public long offset = 0;

    /**
     * The count of bytes that should be used to calculate the end offset when creating the HTTP GET request's Range
     * header. {@code} null is the default and indicates that the entire rest of the blob should be retrieved.
     */
    public Long count = null;

    /**
     * The resource's etag that should be used when creating the HTTP GET request's If-Match header. Note that the
     * Etag is returned with any operation that modifies the resource and by a call to {@link
     * BlobURL#getProperties(BlobAccessConditions)}. Defaults to null.
     */
    public ETag eTag = null;
}
