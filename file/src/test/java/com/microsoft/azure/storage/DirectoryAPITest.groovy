package com.microsoft.azure.storage

import com.microsoft.azure.storage.file.DirectoryURL
import com.microsoft.azure.storage.file.Metadata
import com.microsoft.azure.storage.file.StorageException
import com.microsoft.azure.storage.file.models.DirectoryCreateHeaders
import com.microsoft.azure.storage.file.models.DirectoryCreateResponse
import com.microsoft.azure.storage.file.models.DirectoryDeleteHeaders
import com.microsoft.azure.storage.file.models.DirectoryDeleteResponse
import com.microsoft.azure.storage.file.models.DirectoryGetPropertiesHeaders
import com.microsoft.azure.storage.file.models.DirectoryGetPropertiesResponse
import com.microsoft.azure.storage.file.models.DirectoryItem
import com.microsoft.azure.storage.file.models.DirectoryListFilesAndDirectoriesSegmentResponse
import com.microsoft.azure.storage.file.models.DirectorySetMetadataHeaders
import com.microsoft.azure.storage.file.models.DirectorySetMetadataResponse
import com.microsoft.azure.storage.file.models.StorageErrorCode
import com.microsoft.rest.v2.http.HttpPipeline
import org.junit.Assert
import spock.lang.Unroll

class DirectoryAPITest extends APISpec {

    DirectoryURL du

    def setup() {
        du = shu.createDirectoryURL(generateDirectoryName())
    }

    def "Create directory"() {
        when:
        DirectoryCreateResponse response = du.create(null, null).blockingGet()

        then:
        validateResponseHeaders(response.headers())
        response.statusCode() == 201
    }

    def "Create directory Metadata"() {
        when:
        Metadata metadata = new Metadata()
        metadata.put("key1", "value1")
        metadata.put("key2", "value2")
        DirectoryCreateResponse response = du.create(metadata, null).blockingGet()
        DirectoryGetPropertiesResponse propResponse = du.getProperties(null).blockingGet()

        then:
        validateResponseHeaders(response.headers())
        response.statusCode() == 201
        propResponse.statusCode() == 200
        propResponse.headers().metadata() == metadata
    }

    def "Create Directory Error"() {
        setup:
        shu = su.createShareURL(generateShareName())
        du = shu.createDirectoryURL(generateDirectoryName())

        when:
        du.create(null, null).blockingGet()

        then:
        def e = thrown(StorageException)
        e.response().statusCode() == 404
        e.errorCode() == StorageErrorCode.SHARE_NOT_FOUND
    }

    def "Create Directory already exists"() {
        setup:
        du.create(null, null).blockingGet()

        when:
        du.create(null, null).blockingGet()

        then:
        def e = thrown(StorageException)
        e.response().statusCode() == 409
        e.errorCode() == StorageErrorCode.RESOURCE_ALREADY_EXISTS
    }

    def "Create Directory context"() {
        setup:
        def pipeline = HttpPipeline.build(getStubFactory(getContextStubPolicy(201, DirectoryCreateHeaders)))
        du.withPipeline(pipeline)

        when:
        // No service call is made. Just satisfy the parameters.
        du.create(null, defaultContext).blockingGet()

        then:
        notThrown(RuntimeException)
    }

    def "Delete directory"() {
        setup:
        du.create(null, null).blockingGet()

        when:
        DirectoryDeleteResponse response = du.delete(null).blockingGet()

        then:
        response.headers().version() != null
        response.headers().date() != null
        response.headers().requestId() != null
        response.statusCode() == 202
    }

    def "Delete directory error"() {
        when:
        du.delete(null).blockingGet()

        then:
        def e = thrown(StorageException)
        e.response().statusCode() == 404
        e.errorCode() == StorageErrorCode.RESOURCE_NOT_FOUND
    }

    def "Delete Directory context"() {
        setup:
        def pipeline = HttpPipeline.build(getStubFactory(getContextStubPolicy(202, DirectoryDeleteHeaders)))
        du.withPipeline(pipeline)

        when:
        // No service call is made. Just satisfy the parameters.
        du.create(null, defaultContext).blockingGet()

        then:
        notThrown(RuntimeException)
    }

    def "Get properties "() {
        setup:
        du.create(null, null).blockingGet()

        when:
        DirectoryGetPropertiesResponse response = du.getProperties(null).blockingGet()

        then:
        validateResponseHeaders(response.headers())
        response.statusCode() == 200
        response.headers().metadata().size() == 0
        // By default, data is encrypted using Microsoft Managed Keys
        response.headers().isServerEncrypted()
    }

    def "Get properties validate metadata"() {
        setup:
        Metadata metadata = new Metadata()
        metadata.put("key1", "value1")
        metadata.put("key2", "value2")
        du.create(metadata, null).blockingGet()

        when:
        DirectoryGetPropertiesResponse response = du.getProperties(null).blockingGet()

        then:
        validateResponseHeaders(response.headers())
        response.statusCode() == 200
        response.headers().metadata().size() == 2
        response.headers().metadata() == metadata
        // By default, data is encrypted using Microsoft Managed Keys
        response.headers().isServerEncrypted()
    }

    def "Get properties context"() {
        setup:
        def pipeline = HttpPipeline.build(getStubFactory(getContextStubPolicy(200, DirectoryGetPropertiesHeaders)))
        du.withPipeline(pipeline)

        when:
        // No service call is made. Just satisfy the parameters.
        du.create(null, defaultContext).blockingGet()

        then:
        notThrown(RuntimeException)
    }

    def "Set metadata all null"() {
        when:
        du.create(null, null).blockingGet()
        DirectorySetMetadataResponse response = du.setMetadata(null, null).blockingGet()

        then:
        du.getProperties(null).blockingGet().headers().metadata().size() == 0
        validateBasicHeaders(response.headers())
        response.headers().isServerEncrypted()
    }

    @Unroll
    def "Set metadata metadata"() {
        setup:
        Metadata metadata = new Metadata()
        if (key1 != null && value1 != null) {
            metadata.put(key1, value1)
        }
        if (key2 != null && value2 != null) {
            metadata.put(key2, value2)
        }
        du.create(null, null).blockingGet()

        expect:
        du.setMetadata(metadata, null).blockingGet().statusCode() == statusCode
        du.getProperties(null).blockingGet().headers().metadata() == metadata

        where:
        key1  | value1 | key2   | value2 || statusCode
        null  | null   | null   | null   || 200
        "foo" | "bar"  | "fizz" | "buzz" || 200
    }

    def "Set metadata error"() {
        when:
        DirectorySetMetadataResponse response = du.setMetadata(null, null).blockingGet()

        then:
        def e = thrown(StorageException)
        e.response().statusCode() == 404
        e.errorCode() == StorageErrorCode.RESOURCE_NOT_FOUND
    }

    def "Set metadata context"() {
        setup:
        def pipeline = HttpPipeline.build(getStubFactory(getContextStubPolicy(200, DirectorySetMetadataHeaders)))
        du.withPipeline(pipeline)

        when:
        // No service call is made. Just satisfy the parameters.
        du.create(null, defaultContext).blockingGet()

        then:
        notThrown(RuntimeException)
    }

    def "Director listFilesAndDirectories"() {
        setup:
        du.create(null, null).blockingGet()
        def d1u = du.createDirectoryURL(generateDirectoryName())
        d1u.create(null, null).blockingGet()

        when:
        DirectoryListFilesAndDirectoriesSegmentResponse response = du.listFilesAndDirectoriesSegment(null, null, null, null).blockingGet()

        then:
        validateBasicHeaders(response.headers())
        response.statusCode() == 200
        response.body().entries().directoryItems().size() == 1
        response.body().entries().fileItems().size() == 0
    }

    def "Director listFilesAndDirectories marker"() {
        setup:
        du.create(null, null).blockingGet()
        for (int i = 0; i < 10; i++){
            du.createDirectoryURL(generateDirectoryName()).create(null, null).blockingGet()
        }

        when:
        DirectoryListFilesAndDirectoriesSegmentResponse response = du.listFilesAndDirectoriesSegment(null, null, 6, null).blockingGet()
        String marker = response.body().nextMarker()
        int firstSegmentSize = response.body().entries().directoryItems().size() + response.body().entries().fileItems().size()
        response = du.listFilesAndDirectoriesSegment(null, marker, null, null).blockingGet()

        then:
        validateBasicHeaders(response.headers())
        response.statusCode() == 200
        firstSegmentSize == 6
        response.body().entries().directoryItems().size() + response.body().entries().fileItems().size() == 4
    }

    def "Directory listFilesAndDirectories prefix"() {
        setup:
        du.create(null, null).blockingGet()
        for (int i = 0; i < 10; i++){
            du.createDirectoryURL(generateDirectoryName()).create(null, null).blockingGet()
            du.createFileURL(generateFileName()).create(10, null, null, null).blockingGet()
        }

        when:
        DirectoryListFilesAndDirectoriesSegmentResponse response = du.listFilesAndDirectoriesSegment(directoryPrefix, null, null, null).blockingGet()

        then:
        validateBasicHeaders(response.headers())
        response.statusCode() == 200
        response.body().entries().directoryItems().size() + response.body().entries().fileItems().size() == 10
        for (DirectoryItem directoryItem : response.body().entries().directoryItems()){
            assert directoryItem.name().startsWith(directoryPrefix)
        }
    }

    def "Directory listFilesAndDirectories maxResults"(){
        setup:
        du.create(null, null).blockingGet()
        for (int i = 0; i < 10; i++){
            du.createDirectoryURL(generateDirectoryName()).create(null, null).blockingGet()
            du.createFileURL(generateFileName()).create(10, null, null, null).blockingGet()
        }

        when:
        DirectoryListFilesAndDirectoriesSegmentResponse response = du.listFilesAndDirectoriesSegment(null, null, 10, null).blockingGet()

        then:
        validateBasicHeaders(response.headers())
        response.statusCode() == 200
        response.body().entries().directoryItems().size() + response.body().entries().fileItems().size() == 10
    }
}