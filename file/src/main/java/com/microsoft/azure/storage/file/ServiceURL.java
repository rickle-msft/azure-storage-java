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

import com.microsoft.azure.storage.file.models.ServiceGetPropertiesResponse;
import com.microsoft.azure.storage.file.models.ServiceListSharesSegmentResponse;
import com.microsoft.azure.storage.file.models.ServiceSetPropertiesResponse;
import com.microsoft.azure.storage.file.models.StorageServiceProperties;
import com.microsoft.rest.v2.Context;
import com.microsoft.rest.v2.http.HttpPipeline;
import io.reactivex.Single;

import java.net.MalformedURLException;
import java.net.URL;

import static com.microsoft.azure.storage.file.Utility.addErrorWrappingToSingle;

/**
 * Represents a URL to a file service. This class does not hold any state about a particular storage account but is
 * instead a convenient way of sending off appropriate requests to the resource on the service.
 * It may also be used to construct URLs to share.
 * Please see <a href=https://docs.microsoft.com/en-us/azure/storage/files/storage-files-introduction>here</a> for more
 * information on shares.
 */
public final class ServiceURL extends StorageURL {

    /**
     * Creates a {@code ServiceURL} object pointing to the account specified by the URL and using the provided pipeline
     * to make HTTP requests.
     *
     * @param url
     *         A url to an Azure Storage account.
     * @param pipeline
     *         A pipeline which configures the behavior of HTTP exchanges. Please refer to the createPipeline method on
     *         {@link StorageURL} for more information.
     */
    public ServiceURL(URL url, HttpPipeline pipeline) {
        super(url, pipeline);
    }

    /**
     * Creates a new {@link ServiceURL} with the given pipeline.
     *
     * @param pipeline
     *         An {@link HttpPipeline} object to set.
     *
     * @return A {@link ServiceURL} object with the given pipeline.
     */
    public ServiceURL withPipeline(HttpPipeline pipeline) {
        try {
            return new ServiceURL(new URL(super.storageClient.url()), pipeline);
        } catch (MalformedURLException e) {
            // TODO: remove
        }
        return null;
    }

    /**
     * Creates a {@code ShareURL} object pointing to the account specified by the URL and using the provided
     * pipeline to make HTTP requests.
     *
     * @param shareName
     *         A {@code String} representing the name of the share.
     *
     * @return A new {@link ShareURL} object which references the share with the specified name in this account.
     */
    public ShareURL createShareURL(String shareName) {
        try {
            return new ShareURL(StorageURL.appendToURLPath(new URL(super.storageClient.url()), shareName),
                    super.storageClient.httpPipeline());
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns a single segment of shares starting from the specified Marker. Use an empty marker to start
     * enumeration from the beginning. Share names are returned in lexicographic order. After getting a segment,
     * process it, and then call listSharesSegment again (passing the the previously-returned
     * Marker) to get the next segment. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/list-shares">Azure Docs</a>.
     *
     * @param marker
     *         Identifies the portion of the list to be returned with the next list operation.
     *         This value is returned in the response of a previous list operation as the
     *         ListSharesResponse.body().nextMarker(). Set to null to list the first segment.
     * @param options
     *         A {@link ListSharesOptions} which specifies what data should be returned by the service.
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<ServiceListSharesSegmentResponse> listSharesSegment(String marker,
            ListSharesOptions options, Context context) {
        options = options == null ? ListSharesOptions.DEFAULT : options;
        context = context == null ? Context.NONE : context;

        return addErrorWrappingToSingle(
                this.storageClient.generatedServices().listSharesSegmentWithRestResponseAsync(context,
                        options.prefix(), marker, options.maxResults(), options.details().toIncludeType(),
                        null));
    }

    /**
     * Gets the properties of a storage account’s file service endpoint. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/get-file-service-properties">Azure Docs</a>.
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
    public Single<ServiceGetPropertiesResponse> getProperties(Context context) {
        context = context == null ? Context.NONE : context;

        return addErrorWrappingToSingle(
                this.storageClient.generatedServices().getPropertiesWithRestResponseAsync(context, null));
    }

    /**
     * Sets properties for a storage account's File service endpoint. For more information, see the
     * <a href="https://docs.microsoft.com/en-us/rest/api/storageservices/set-file-service-properties">Azure Docs</a>.
     * Note that setting the default service version has no effect when using this client because this client explicitly
     * sets the version header on each request, overriding the default.
     *
     * @param properties
     *         Configures the service.
     * @param context
     *         {@code Context} offers a means of passing arbitrary data (key/value pairs) to an
     *         {@link com.microsoft.rest.v2.http.HttpPipeline}'s policy objects. Most applications do not need to pass
     *         arbitrary data to the pipeline and can pass {@code Context.NONE} or {@code null}. Each context object is
     *         immutable. The {@code withContext} with data method creates a new {@code Context} object that refers to its
     *         parent, forming a linked list.
     *
     * @return Emits the successful response.
     */
    public Single<ServiceSetPropertiesResponse> setProperties(StorageServiceProperties properties, Context context) {
        context = context == null ? Context.NONE : context;

        return addErrorWrappingToSingle(
                this.storageClient.generatedServices().setPropertiesWithRestResponseAsync(context, properties, null));
    }
}
