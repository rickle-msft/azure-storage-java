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

import com.microsoft.rest.v2.http.UrlBuilder;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;


/**
 * A FileURLParts object represents the components that make up an Azure Storage Share/Directory/File URL. You may parse an
 * existing URL into its parts with the  class. You may construct a URL from parts by calling toURL().
 * It is also possible to use the empty constructor to build a queueURL from scratch.
 * NOTE: Changing any SAS-related field requires computing a new SAS signature.
 */
public final class FileURLParts {

    private String scheme;

    private String host;

    private String shareName;

    private String directoryOrFilePath;

    private String shareSnapshot;

    private SASQueryParameters sasQueryParameters;

    private Map<String, String[]> unparsedParameters;

    private IPEndPointStyleInfo ipEndPointStyleInfo;

    /**
     * The scheme. Ex: "https://".
     */
    public String scheme() {
        return scheme;
    }

    /**
     * The scheme. Ex: "https://".
     */
    public FileURLParts withScheme(String scheme) {
        this.scheme = scheme;
        return this;
    }

    /**
     * The host. Ex: "account.share.core.windows.net".
     */
    public String host() {
        return host;
    }

    /**
     * The host. Ex: "account.share.core.windows.net".
     */
    public FileURLParts withHost(String host) {
        this.host = host;
        return this;
    }

    /**
     * The shareName name or {@code null} if a {@link ServiceURL} was parsed.
     */
    public String shareName() {
        return shareName;
    }

    /**
     * The shareName name or {@code null} if a {@link ServiceURL} was parsed.
     */
    public FileURLParts withShareName(String shareName) {
        this.shareName = shareName;
        return this;
    }

    /**
     * The path of directory or file or {@code null} if a {@Link ServiceURL} or {@Link ShareURL} was parsed.
     */
    public String directoryOrFilePath() {
        return directoryOrFilePath;
    }

    /**
     * The path of directory or file or {@code null} if a {@Link ServiceURL} or {@Link ShareURL} was parsed..
     */
    public FileURLParts withDirectoryOrFilePath(String directoryOrFilePath) {
        this.directoryOrFilePath = directoryOrFilePath;
        return this;
    }

    /**
     * The snapshot time or {@code null} if anything except a URL to a snapshot was parsed.
     */
    public String shareSnapshot() {
        return shareSnapshot;
    }

    /**
     * The snapshot time or {@code null} if anything except a URL to a snapshot was parsed.
     */
    public FileURLParts withShareSnapshot(String shareSnapshot) {
        this.shareSnapshot = shareSnapshot;
        return this;
    }

    /**
     * A {@link SASQueryParameters} representing the SAS query parameters or {@code null} if there were no such
     * parameters.
     */
    public SASQueryParameters sasQueryParameters() {
        return sasQueryParameters;
    }

    /**
     * A {@link SASQueryParameters} representing the SAS query parameters or {@code null} if there were no such
     * parameters.
     */
    public FileURLParts withSasQueryParameters(SASQueryParameters sasQueryParameters) {
        this.sasQueryParameters = sasQueryParameters;
        return this;
    }

    /**
     * A {@Link IPEndPointStyleInfo}
     */
    public IPEndPointStyleInfo ipEndPointStyleInfo() {
        return ipEndPointStyleInfo;
    }

    /**
     * A {@Link IPEndPointStyleInfo}
     */
    public FileURLParts withIPEndPointStyleInfo(IPEndPointStyleInfo ipEndPointStyleInfo) {
        this.ipEndPointStyleInfo = ipEndPointStyleInfo;
        return this;
    }

    /**
     * The query parameter key value pairs aside from SAS parameters or {@code null} if there were
     * no such parameters.
     */
    public Map<String, String[]> unparsedParameters() {
        return unparsedParameters;
    }

    /**
     * The query parameter key value pairs aside from SAS parameters or {@code null} if there were
     * no such parameters.
     */
    public FileURLParts withUnparsedParameters(Map<String, String[]> unparsedParameters) {
        this.unparsedParameters = unparsedParameters;
        return this;
    }

    /**
     * Initializes a FileURLParts object with all fields set to null, except unparsedParameters, which is an empty map.
     * This may be useful for constructing a URL to a file storage resource from scratch when the constituent parts are
     * already known.
     */
    public FileURLParts() {
        unparsedParameters = new HashMap<>();
    }

    /**
     * Converts the file URL parts to a {@link URL}.
     *
     * @return A {@code java.net.URL} to the queue resource composed of all the elements in the object.
     *
     * @throws MalformedURLException
     *         The fields present on the FileURLParts object were insufficient to construct a valid URL or were
     *         ill-formatted.
     */
    public URL toURL() throws MalformedURLException {
        UrlBuilder url = new UrlBuilder().withScheme(this.scheme).withHost(this.host);

        StringBuilder path = new StringBuilder();

        if (this.ipEndPointStyleInfo != null) {
            if (this.ipEndPointStyleInfo.accountName() != null) {
                /* Added a path separator after the account name. Anything that is added
                 after the account name doesn't need to care about the "/". */
                path.append(this.ipEndPointStyleInfo.accountName() + "/");
            }
            if (this.ipEndPointStyleInfo.port() != null) {
                url.withPort(this.ipEndPointStyleInfo.port());
            }
        }

        // Concatenate share & path of directory or file (if they exist)
        if (shareNameSpecified()) {
            path.append(this.shareName);
            if (directoryOrFilePathSpecified()) {
                // If messages is set to true, append "messages" keyword to the Url.
                path.append("/" + this.directoryOrFilePath);
            }
        }

        url.withPath(path.toString());

        // Concatenate share snapshot query parameter (if it exists)
        if (this.shareSnapShotSpecified()) {
            url.setQueryParameter(Constants.SNAPSHOT_QUERY_PARAMETER, this.shareSnapshot);
        }

        if (this.sasQueryParameters != null) {
            String encodedSAS = this.sasQueryParameters.encode();
            if (encodedSAS.length() != 0) {
                url.withQuery(encodedSAS);
            }
        }

        for (Map.Entry<String, String[]> entry : this.unparsedParameters.entrySet()) {
            // The commas are intentionally encoded.
            url.setQueryParameter(entry.getKey(),
                    Utility.safeURLEncode(String.join(",", entry.getValue())));
        }
        return url.toURL();
    }

    /**
     * shareSnapShotSpecified function returns whether shareSnapShot is specified in fileURLParts
     * or not.
     */
    private boolean shareSnapShotSpecified() {
        if (this.shareSnapshot != null && this.shareSnapshot.length() > 0) {
            return true;
        }
        return false;
    }

    /**
     * directoryOrFilePathSpecified function returns whether directoryOrFilePath is specified in
     * fileURLParts or not.
     */
    private boolean directoryOrFilePathSpecified() {
        if (this.directoryOrFilePath == null || directoryOrFilePath.length() == 0) {
            return false;
        }
        return true;
    }

    /**
     * shareNameSpecified function returns whether shareName is specified in fileURLParts
     * or not.
     */
    private boolean shareNameSpecified() {
        if (this.shareName != null && this.shareName.length() > 0) {
            return true;
        }
        return false;
    }
}
