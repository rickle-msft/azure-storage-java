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
 * Defines options available to configure the behavior of a call to listSharesSegment on a {@link ServiceURL}
 * object. See the constructor for details on each of the options. Null may be passed in place of an object of this
 * type if no options are desirable.
 */
public final class ListSharesOptions {

    /**
     * An object representing the default options: no details or prefix and using the service's default for maxResults.
     */
    public static final ListSharesOptions DEFAULT =
            new ListSharesOptions();

    private ShareListingDetails details;

    private String prefix;

    private Integer maxResults;

    /**
     * {@link ShareListingDetails}
     */
    public ShareListingDetails details() {
        return details;
    }

    /**
     * {@link ShareListingDetails}
     */
    public ListSharesOptions withDetails(ShareListingDetails details) {
        this.details = details;
        return this;
    }

    /**
     * Filters the results to return shares whose names begin with the specified prefix.     *
     */
    public String prefix() {
        return prefix;
    }

    /**
     * Filters the results to return shares whose names begin with the specified prefix.     *
     */
    public ListSharesOptions withPrefix(String prefix) {
        this.prefix = prefix;
        return this;
    }

    /**
     * Specifies the maximum number of shares to return, including all sharePrefix elements. If the request does not
     * specify maxResults or specifies a value greater than 5,000, the server will return up to 5,000 items.
     */
    public Integer maxResults() {
        return maxResults;
    }

    /**
     * Specifies the maximum number of shares to return, including all sharePrefix elements. If the request does not
     * specify maxResults or specifies a value greater than 5,000, the server will return up to 5,000 items.
     */
    public ListSharesOptions withMaxResults(Integer maxResults) {
        if (maxResults != null && maxResults <= 0) {
            throw new IllegalArgumentException("MaxResults must be greater than 0.");
        }
        this.maxResults = maxResults;
        return this;
    }

    public ListSharesOptions() {
        this.details = ShareListingDetails.NONE;
    }
}
