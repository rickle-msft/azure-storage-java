package com.microsoft.azure.storage

import com.microsoft.azure.storage.file.*
import com.microsoft.azure.storage.file.models.*
import com.microsoft.rest.v2.http.HttpPipeline

class ServiceAPITest extends APISpec {
    ServiceURL su
    ShareURL shu

    def setup() {
        APISpec.cleanupShares()
        su = primaryServiceURL
    }

    def "List Share Segment"() {
        setup:
        shu = su.createShareURL(generateShareName())

        when:
        ShareCreateResponse sResponse = shu.create(null, null, null).blockingGet()
        ServiceListSharesSegmentResponse lResponse =
                su.listSharesSegment(null, new ListSharesOptions().withPrefix(sharePrefix), null).blockingGet()

        then:
        sResponse.statusCode() == 201
        validateResponseHeaders(sResponse.headers())
        lResponse.body().shareItems().size() == 1
        for (ShareItem queueItem : lResponse.body().shareItems()) {
            queueItem.name().startsWith(sharePrefix)
        }
    }

    def "List Share Marker"() {
        setup:
        for (int i = 0; i < 10; i++) {
            ShareURL shUrl = primaryServiceURL.createShareURL(generateShareName())
            shUrl.create(null, null, null).blockingGet()
        }

        ServiceListSharesSegmentResponse response =
                primaryServiceURL.listSharesSegment(null,
                        new ListSharesOptions().withMaxResults(5), null).blockingGet()
        String marker = response.body().nextMarker()
        String firstShareName = response.body().shareItems().get(0).name()
        response = primaryServiceURL.listSharesSegment(marker,
                new ListSharesOptions().withMaxResults(5), null).blockingGet()

        expect:
        // Assert that the second segment is indeed after the first alphabetically
        firstShareName < response.body().shareItems().get(0).name()
    }

    def "List share details"() {
        setup:
        Metadata metadata = new Metadata()
        metadata.put("foo", "bar")
        shu = primaryServiceURL.createShareURL("aaa" + generateShareName())
        shu.create(metadata, null, null).blockingGet()

        expect:
        primaryServiceURL.listSharesSegment(null,
                new ListSharesOptions().withDetails(new ShareListingDetails().withMetadata(true))
                        .withPrefix("aaa" + sharePrefix), null).blockingGet().body().shareItems()
                .get(0).metadata() == metadata
        // Share with prefix "aaa" will not be cleaned up by normal test cleanup.
        shu.delete(null, null).blockingGet().statusCode() == 202
    }

    def "List shares maxResults"() {
        setup:
        for (int i = 0; i < 11; i++) {
            primaryServiceURL.createShareURL(generateShareName()).create(null, null, null)
                    .blockingGet()
        }
        expect:
        primaryServiceURL.listSharesSegment(null,
                new ListSharesOptions().withMaxResults(10), null)
                .blockingGet().body().shareItems().size() == 10
    }

    def "List share error"() {
        when:
        primaryServiceURL.listSharesSegment("garbage", null, null).blockingGet()

        then:
        thrown(StorageException)
    }

    def "List share context"() {
        setup:
        def pipeline =
                HttpPipeline.build(getStubFactory(getContextStubPolicy(200, ServiceListSharesSegmentHeaders)))

        def su = primaryServiceURL.withPipeline(pipeline)

        when:
        // No service call is made. Just satisfy the parameters.
        su.listSharesSegment(null, null, defaultContext)

        then:
        notThrown(RuntimeException)
    }

    def validatePropsSet(ServiceSetPropertiesHeaders headers, StorageServiceProperties receivedProperties) {
        return headers.requestId() != null &&
                headers.version() != null &&

                receivedProperties.cors().size() == 1 &&
                receivedProperties.cors().get(0).allowedMethods() == "GET,PUT,HEAD" &&
                receivedProperties.cors().get(0).allowedHeaders() == "x-ms-version" &&
                receivedProperties.cors().get(0).allowedOrigins() == "*" &&
                receivedProperties.cors().get(0).exposedHeaders() == "x-ms-client-request-id" &&
                receivedProperties.cors().get(0).maxAgeInSeconds() == 10 &&

                receivedProperties.hourMetrics().enabled() &&
                receivedProperties.hourMetrics().includeAPIs() &&
                receivedProperties.hourMetrics().retentionPolicy().enabled() &&
                receivedProperties.hourMetrics().retentionPolicy().days() == 5 &&
                receivedProperties.hourMetrics().version() == "1.0" &&

                receivedProperties.minuteMetrics().enabled() &&
                receivedProperties.minuteMetrics().includeAPIs() &&
                receivedProperties.minuteMetrics().retentionPolicy().enabled() &&
                receivedProperties.minuteMetrics().retentionPolicy().days() == 5 &&
                receivedProperties.minuteMetrics().version() == "1.0"
    }

    def "Set get properties"() {
        when:
        RetentionPolicy retentionPolicy = new RetentionPolicy().withDays(5).withEnabled(true)

        ArrayList<CorsRule> corsRules = new ArrayList<>()
        corsRules.add(new CorsRule().withAllowedMethods("GET,PUT,HEAD")
                .withAllowedOrigins("*")
                .withAllowedHeaders("x-ms-version")
                .withExposedHeaders("x-ms-client-request-id")
                .withMaxAgeInSeconds(10))

        Metrics hourMetrics = new Metrics().withEnabled(true).withVersion("1.0")
                .withRetentionPolicy(retentionPolicy).withIncludeAPIs(true)
        Metrics minuteMetrics = new Metrics().withEnabled(true).withVersion("1.0")
                .withRetentionPolicy(retentionPolicy).withIncludeAPIs(true)

        StorageServiceProperties sentProperties = new StorageServiceProperties()
                .withMinuteMetrics(minuteMetrics).withHourMetrics(hourMetrics)
                .withCors(corsRules)

        ServiceSetPropertiesHeaders headers = primaryServiceURL.setProperties(sentProperties, null)
                .blockingGet().headers()

        // Service properties may take up to 30s to take effect. If they weren't already in place, wait.
        sleep(30 * 1000)

        StorageServiceProperties receivedProperties = primaryServiceURL.getProperties(null)
                .blockingGet().body()

        then:
        headers.requestId() != null
        headers.version() != null
        validatePropsSet(headers, receivedProperties)
    }

    // In java, we don't have support from the validator for checking the bounds on days. The service will catch these.

    def "Set props error"() {
        when:
        new ServiceURL(new URL("https://error.file.core.windows.net"),
                StorageURL.createPipeline(primaryCreds, new PipelineOptions()))
                .setProperties(new StorageServiceProperties(), null).blockingGet()

        then:
        thrown(StorageException)
    }

    def "Set props context"() {
        setup:
        def pipeline =
                HttpPipeline.build(getStubFactory(getContextStubPolicy(200, ServiceSetPropertiesHeaders)))

        def su = primaryServiceURL.withPipeline(pipeline)

        when:
        // No service call is made. Just satisfy the parameters.
        su.setProperties(new StorageServiceProperties(), defaultContext)

        then:
        notThrown(RuntimeException)
    }

    def "Get props error"() {
        when:
        new ServiceURL(new URL("https://error.file.core.windows.net"),
                StorageURL.createPipeline(primaryCreds, new PipelineOptions())).getProperties(null).blockingGet()

        then:
        thrown(StorageException)
    }

    def "Get props context"() {
        setup:
        def pipeline =
                HttpPipeline.build(getStubFactory(getContextStubPolicy(200, ServiceGetPropertiesHeaders)))

        def su = primaryServiceURL.withPipeline(pipeline)

        when:
        // No service call is made. Just satisfy the parameters.
        su.getProperties(defaultContext)

        then:
        notThrown(RuntimeException)
    }
}
