/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for
 * license information.
 *
 * Code generated by Microsoft (R) AutoRest Code Generator.
 * Changes may cause incorrect behavior and will be lost if the code is
 * regenerated.
 */

package com.microsoft.azure.storage.blob.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

/**
 * An enumeration of blobs.
 */
@JacksonXmlRootElement(localName = "EnumerationResults")
public final class ListBlobsHierarchyResponse {
    /**
     * The serviceEndpoint property.
     */
    @JacksonXmlProperty(localName = "ServiceEndpoint", isAttribute = true)
    private String serviceEndpoint;

    /**
     * The containerName property.
     */
    @JacksonXmlProperty(localName = "ContainerName", isAttribute = true)
    private String containerName;

    /**
     * The prefix property.
     */
    @JsonProperty(value = "Prefix", required = true)
    private String prefix;

    /**
     * The marker property.
     */
    @JsonProperty(value = "Marker", required = true)
    private String marker;

    /**
     * The maxResults property.
     */
    @JsonProperty(value = "MaxResults", required = true)
    private int maxResults;

    /**
     * The delimiter property.
     */
    @JsonProperty(value = "Delimiter", required = true)
    private String delimiter;

    /**
     * The blobs property.
     */
    @JsonProperty(value = "Blobs", required = true)
    private BlobHierarchyList blobs;

    /**
     * The nextMarker property.
     */
    @JsonProperty(value = "NextMarker", required = true)
    private String nextMarker;

    /**
     * Get the serviceEndpoint value.
     *
     * @return the serviceEndpoint value.
     */
    public String serviceEndpoint() {
        return this.serviceEndpoint;
    }

    /**
     * Set the serviceEndpoint value.
     *
     * @param serviceEndpoint the serviceEndpoint value to set.
     * @return the ListBlobsHierarchyResponse object itself.
     */
    public ListBlobsHierarchyResponse withServiceEndpoint(String serviceEndpoint) {
        this.serviceEndpoint = serviceEndpoint;
        return this;
    }

    /**
     * Get the containerName value.
     *
     * @return the containerName value.
     */
    public String containerName() {
        return this.containerName;
    }

    /**
     * Set the containerName value.
     *
     * @param containerName the containerName value to set.
     * @return the ListBlobsHierarchyResponse object itself.
     */
    public ListBlobsHierarchyResponse withContainerName(String containerName) {
        this.containerName = containerName;
        return this;
    }

    /**
     * Get the prefix value.
     *
     * @return the prefix value.
     */
    public String prefix() {
        return this.prefix;
    }

    /**
     * Set the prefix value.
     *
     * @param prefix the prefix value to set.
     * @return the ListBlobsHierarchyResponse object itself.
     */
    public ListBlobsHierarchyResponse withPrefix(String prefix) {
        this.prefix = prefix;
        return this;
    }

    /**
     * Get the marker value.
     *
     * @return the marker value.
     */
    public String marker() {
        return this.marker;
    }

    /**
     * Set the marker value.
     *
     * @param marker the marker value to set.
     * @return the ListBlobsHierarchyResponse object itself.
     */
    public ListBlobsHierarchyResponse withMarker(String marker) {
        this.marker = marker;
        return this;
    }

    /**
     * Get the maxResults value.
     *
     * @return the maxResults value.
     */
    public int maxResults() {
        return this.maxResults;
    }

    /**
     * Set the maxResults value.
     *
     * @param maxResults the maxResults value to set.
     * @return the ListBlobsHierarchyResponse object itself.
     */
    public ListBlobsHierarchyResponse withMaxResults(int maxResults) {
        this.maxResults = maxResults;
        return this;
    }

    /**
     * Get the delimiter value.
     *
     * @return the delimiter value.
     */
    public String delimiter() {
        return this.delimiter;
    }

    /**
     * Set the delimiter value.
     *
     * @param delimiter the delimiter value to set.
     * @return the ListBlobsHierarchyResponse object itself.
     */
    public ListBlobsHierarchyResponse withDelimiter(String delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    /**
     * Get the blobs value.
     *
     * @return the blobs value.
     */
    public BlobHierarchyList blobs() {
        return this.blobs;
    }

    /**
     * Set the blobs value.
     *
     * @param blobs the blobs value to set.
     * @return the ListBlobsHierarchyResponse object itself.
     */
    public ListBlobsHierarchyResponse withBlobs(BlobHierarchyList blobs) {
        this.blobs = blobs;
        return this;
    }

    /**
     * Get the nextMarker value.
     *
     * @return the nextMarker value.
     */
    public String nextMarker() {
        return this.nextMarker;
    }

    /**
     * Set the nextMarker value.
     *
     * @param nextMarker the nextMarker value to set.
     * @return the ListBlobsHierarchyResponse object itself.
     */
    public ListBlobsHierarchyResponse withNextMarker(String nextMarker) {
        this.nextMarker = nextMarker;
        return this;
    }
}
