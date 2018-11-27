package com.microsoft.azure.storage

import com.microsoft.azure.storage.file.Metadata
import com.microsoft.azure.storage.file.StorageException
import com.microsoft.azure.storage.file.models.AccessPolicy
import com.microsoft.azure.storage.file.models.ShareCreateHeaders
import com.microsoft.azure.storage.file.models.ShareCreateResponse
import com.microsoft.azure.storage.file.models.ShareDeleteHeaders
import com.microsoft.azure.storage.file.models.ShareDeleteResponse
import com.microsoft.azure.storage.file.models.ShareGetAccessPolicyHeaders
import com.microsoft.azure.storage.file.models.ShareGetAccessPolicyResponse
import com.microsoft.azure.storage.file.models.ShareGetPropertiesHeaders
import com.microsoft.azure.storage.file.models.ShareGetPropertiesResponse
import com.microsoft.azure.storage.file.models.ShareSetAccessPolicyHeaders
import com.microsoft.azure.storage.file.models.ShareSetAccessPolicyResponse
import com.microsoft.azure.storage.file.models.ShareSetMetadataHeaders
import com.microsoft.azure.storage.file.models.ShareSetMetadataResponse
import com.microsoft.azure.storage.file.models.SignedIdentifier
import com.microsoft.azure.storage.file.models.StorageErrorCode
import com.microsoft.rest.v2.http.HttpPipeline
import spock.lang.Unroll

import java.time.OffsetDateTime
import java.time.ZoneId

class ShareAPITest extends APISpec {

    def "Create all null"() {
        setup:
        // Overwrite the existing shu, which has already been created
        shu = primaryServiceURL.createShareURL(generateShareName())

        when:
        ShareCreateResponse response = shu.create(null, null, null).blockingGet()

        then:
        response.statusCode() == 201
        validateResponseHeaders(response.headers())
    }

    @Unroll
    def "Create metadata"() {
        setup:
        shu = primaryServiceURL.createShareURL(generateShareName())
        Metadata metadata = new Metadata()
        if (key1 != null) {
            metadata.put(key1, value1)
        }
        if (key2 != null) {
            metadata.put(key2, value2)
        }

        when:
        shu.create(metadata, null, null).blockingGet()
        ShareGetPropertiesResponse response = shu.getProperties(null).blockingGet()

        then:
        response.headers().metadata() == metadata

        where:
        key1  | value1 | key2   | value2
        null  | null   | null   | null
        "foo" | "bar"  | "fizz" | "buzz"
    }

    def "Create error"() {
        when:
        shu.create(null, null, null).blockingGet()

        then:
        def e = thrown(StorageException)
        e.response().statusCode() == 409
        e.errorCode() == StorageErrorCode.SHARE_ALREADY_EXISTS
        e.message().contains("The specified share already exists.")
    }

    def "Create context"() {
        setup:
        def pipeline = HttpPipeline.build(getStubFactory(getContextStubPolicy(201, ShareCreateHeaders)))

        shu = shu.withPipeline(pipeline)

        when:
        // No service call is made. Just satisfy the parameters.
        shu.create(null, null, defaultContext).blockingGet()

        then:
        notThrown(RuntimeException)
    }

    def "Get properties null"() {
        when:
        ShareGetPropertiesHeaders headers =
                shu.getProperties(null).blockingGet().headers()

        then:
        validateResponseHeaders(headers)
        headers.quota() > 0
        headers.metadata().size() == 0
    }

    def "Get properties error"() {
        setup:
        shu = primaryServiceURL.createShareURL(generateShareName())

        when:
        shu.getProperties(null).blockingGet()

        then:
        thrown(StorageException)
    }

    def "Get properties context"() {
        setup:
        def pipeline = HttpPipeline.build(getStubFactory(getContextStubPolicy(200, ShareGetPropertiesHeaders)))

        shu = shu.withPipeline(pipeline)

        when:
        // No service call is made. Just satisfy the parameters.
        shu.getProperties(defaultContext).blockingGet()

        then:
        notThrown(RuntimeException)
    }

    def "Set metadata"() {
        setup:
        shu = primaryServiceURL.createShareURL(generateShareName())
        Metadata metadata = new Metadata()
        metadata.put("key", "value")
        shu.create(metadata, null, null).blockingGet()
        ShareSetMetadataResponse response = shu.setMetadata(metadata, null).blockingGet()

        expect:
        response.statusCode() == 200
        validateResponseHeaders(response.headers())
        shu.getProperties(null).blockingGet().headers().metadata().size() == 1
    }

    @Unroll
    def "Set metadata metadata"() {
        setup:
        Metadata metadata = new Metadata()
        if (key1 != null) {
            metadata.put(key1, value1)
        }
        if (key2 != null) {
            metadata.put(key2, value2)
        }

        expect:
        shu.setMetadata(metadata, null).blockingGet().statusCode() == 200
        shu.getProperties(null).blockingGet().headers().metadata() == metadata

        where:
        key1  | value1 | key2   | value2
        null  | null   | null   | null
        "foo" | "bar"  | "fizz" | "buzz"
    }

    def "Set metadata error"() {
        setup:
        shu = primaryServiceURL.createShareURL(generateShareName())

        when:
        shu.setMetadata(null, null).blockingGet()

        then:
        thrown(StorageException)
    }

    def "Set metadata context"() {
        setup:
        def pipeline = HttpPipeline.build(getStubFactory(getContextStubPolicy(200, ShareSetMetadataHeaders)))

        shu = shu.withPipeline(pipeline)

        when:
        // No service call is made. Just satisfy the parameters.
        shu.setMetadata(null, defaultContext).blockingGet()

        then:
        notThrown(RuntimeException)
    }

    def "Set access policy ids"() {
        setup:
        shu = primaryServiceURL.createShareURL(generateShareName())
        shu.create(null, null, null).blockingGet()
        SignedIdentifier identifier = new SignedIdentifier()
                .withId("0000")
                .withAccessPolicy(new AccessPolicy()
                .withStart(OffsetDateTime.now().atZoneSameInstant(ZoneId.of("UTC")).toOffsetDateTime())
                .withExpiry(OffsetDateTime.now().atZoneSameInstant(ZoneId.of("UTC")).toOffsetDateTime()
                .plusDays(1))
                .withPermission("l"))
        SignedIdentifier identifier2 = new SignedIdentifier()
                .withId("0001")
                .withAccessPolicy(new AccessPolicy()
                .withStart(OffsetDateTime.now().atZoneSameInstant(ZoneId.of("UTC")).toOffsetDateTime())
                .withExpiry(OffsetDateTime.now().atZoneSameInstant(ZoneId.of("UTC")).toOffsetDateTime()
                .plusDays(2))
                .withPermission("c"))
        List<SignedIdentifier> ids = new ArrayList<>()
        ids.push(identifier)
        ids.push(identifier2)

        when:
        ShareSetAccessPolicyResponse response =
                shu.setAccessPolicy(ids, null).blockingGet()
        List<SignedIdentifier> receivedIdentifiers = shu.getAccessPolicy(null).blockingGet().body()

        then:
        response.statusCode() == 200
        validateResponseHeaders(response.headers())
        receivedIdentifiers.get(0).accessPolicy().expiry() == identifier.accessPolicy().expiry()
        receivedIdentifiers.get(0).accessPolicy().start() == identifier.accessPolicy().start()
        receivedIdentifiers.get(0).accessPolicy().permission() == identifier.accessPolicy().permission()
        receivedIdentifiers.get(1).accessPolicy().expiry() == identifier2.accessPolicy().expiry()
        receivedIdentifiers.get(1).accessPolicy().start() == identifier2.accessPolicy().start()
        receivedIdentifiers.get(1).accessPolicy().permission() == identifier2.accessPolicy().permission()
    }

    def "Set access policy error"() {
        setup:
        shu = primaryServiceURL.createShareURL(generateShareName())

        when:
        shu.setAccessPolicy(null, null).blockingGet()

        then:
        thrown(StorageException)
    }

    def "Set access policy context"() {
        setup:
        def pipeline = HttpPipeline.build(getStubFactory(getContextStubPolicy(200, ShareSetAccessPolicyHeaders)))

        shu = shu.withPipeline(pipeline)

        when:
        // No service call is made. Just satisfy the parameters.
        shu.setAccessPolicy(null, defaultContext).blockingGet()

        then:
        notThrown(RuntimeException)
    }

    def "Get access policy"() {
        setup:
        SignedIdentifier identifier = new SignedIdentifier()
                .withId("0000")
                .withAccessPolicy(new AccessPolicy()
                .withStart(OffsetDateTime.now().atZoneSameInstant(ZoneId.of("UTC")).toOffsetDateTime())
                .withExpiry(OffsetDateTime.now().atZoneSameInstant(ZoneId.of("UTC")).toOffsetDateTime()
                .plusDays(1))
                .withPermission("r"))
        List<SignedIdentifier> ids = new ArrayList<>()
        ids.push(identifier)
        shu.setAccessPolicy(ids, null).blockingGet()
        ShareGetAccessPolicyResponse response = shu.getAccessPolicy(null).blockingGet()

        expect:
        response.statusCode() == 200
        validateResponseHeaders(response.headers())
        response.body().get(0).accessPolicy().expiry() == identifier.accessPolicy().expiry()
        response.body().get(0).accessPolicy().start() == identifier.accessPolicy().start()
        response.body().get(0).accessPolicy().permission() == identifier.accessPolicy().permission()
    }

    def "Get access policy error"() {
        setup:
        shu = primaryServiceURL.createShareURL(generateShareName())

        when:
        shu.getAccessPolicy(null).blockingGet()

        then:
        thrown(StorageException)
    }

    def "Get access policy context"() {
        setup:
        def pipeline = HttpPipeline.build(getStubFactory(getContextStubPolicy(200, ShareGetAccessPolicyHeaders)))

        shu = shu.withPipeline(pipeline)

        when:
        // No service call is made. Just satisfy the parameters.
        shu.getAccessPolicy(defaultContext).blockingGet()

        then:
        notThrown(RuntimeException)
    }

    def "Delete"() {
        when:
        ShareDeleteResponse response = shu.delete(null, null).blockingGet()

        then:
        response.statusCode() == 202
        response.headers().requestId() != null
        response.headers().version() != null
        response.headers().date() != null
    }

    def "Delete error"() {
        setup:
        shu = primaryServiceURL.createShareURL(generateShareName())

        when:
        shu.delete(null, null).blockingGet()

        then:
        thrown(StorageException)
    }

    def "Delete context"() {
        setup:
        def pipeline = HttpPipeline.build(getStubFactory(getContextStubPolicy(202, ShareDeleteHeaders)))

        shu = shu.withPipeline(pipeline)

        when:
        // No service call is made. Just satisfy the parameters.
        shu.delete(null, defaultContext).blockingGet()

        then:
        notThrown(RuntimeException)
    }

}
