/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for
 * license information.
 *
 * Code generated by Microsoft (R) AutoRest Code Generator.
 * Changes may cause incorrect behavior and will be lost if the code is
 * regenerated.
 */

package com.microsoft.azure.storage.file.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

/**
 * Abstract for entries that can be listed from Directory.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "EntryType", defaultImpl = Entry.class)
@JsonTypeName("Entry")
@JsonSubTypes({
    @JsonSubTypes.Type(name = "Directory", value = DirectoryItem.class),
    @JsonSubTypes.Type(name = "File", value = FileItem.class)
})
@JacksonXmlRootElement(localName = "Entry")
public class Entry {
    /**
     * Name of the entry.
     */
    @JsonProperty(value = "Name", required = true)
    private String name;

    /**
     * Get the name value.
     *
     * @return the name value.
     */
    public String name() {
        return this.name;
    }

    /**
     * Set the name value.
     *
     * @param name the name value to set.
     * @return the Entry object itself.
     */
    public Entry withName(String name) {
        this.name = name;
        return this;
    }
}
