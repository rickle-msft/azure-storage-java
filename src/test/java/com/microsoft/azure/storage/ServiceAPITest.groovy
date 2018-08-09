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

package com.microsoft.azure.storage

import com.microsoft.azure.storage.blob.BlobURLParts
import com.microsoft.azure.storage.blob.ContainerListingDetails
import com.microsoft.azure.storage.blob.ContainerURL
import com.microsoft.azure.storage.blob.ListContainersOptions
import com.microsoft.azure.storage.blob.Metadata
import com.microsoft.azure.storage.blob.PipelineOptions
import com.microsoft.azure.storage.blob.ServiceURL
import com.microsoft.azure.storage.blob.StorageException
import com.microsoft.azure.storage.blob.StorageURL
import com.microsoft.azure.storage.blob.URLParser
import com.microsoft.azure.storage.blob.models.ContainerItem
import com.microsoft.azure.storage.blob.models.CorsRule
import com.microsoft.azure.storage.blob.models.Logging
import com.microsoft.azure.storage.blob.models.Metrics
import com.microsoft.azure.storage.blob.models.RetentionPolicy
import com.microsoft.azure.storage.blob.models.ServiceGetStatisticsResponse
import com.microsoft.azure.storage.blob.models.ServiceListContainersSegmentResponse
import com.microsoft.azure.storage.blob.models.ServiceSetPropertiesHeaders
import com.microsoft.azure.storage.blob.models.StaticWebsite
import com.microsoft.azure.storage.blob.models.StorageServiceProperties
import spock.lang.Unroll

import java.lang.annotation.Retention

class ServiceAPITest extends APISpec {
    def setup() {
        RetentionPolicy disabled = new RetentionPolicy().withEnabled(false)
        primaryServiceURL.setProperties(new StorageServiceProperties()
                .withStaticWebsite(new StaticWebsite().withEnabled(false))
                .withDeleteRetentionPolicy(disabled)
                .withCors(null)
                .withHourMetrics(new Metrics().withVersion("1.0").withEnabled(false)
                .withRetentionPolicy(disabled))
                .withMinuteMetrics(new Metrics().withVersion("1.0").withEnabled(false)
                .withRetentionPolicy(disabled))
                .withLogging(new Logging().withVersion("1.0")
                .withRetentionPolicy(disabled))
                .withDefaultServiceVersion("2018-03-28")).blockingGet()
    }

    def cleanup() {
        RetentionPolicy disabled = new RetentionPolicy().withEnabled(false)
        primaryServiceURL.setProperties(new StorageServiceProperties()
                .withStaticWebsite(new StaticWebsite().withEnabled(false))
                .withDeleteRetentionPolicy(disabled)
                .withCors(null)
                .withHourMetrics(new Metrics().withVersion("1.0").withEnabled(false)
                .withRetentionPolicy(disabled))
                .withMinuteMetrics(new Metrics().withVersion("1.0").withEnabled(false)
                .withRetentionPolicy(disabled))
                .withLogging(new Logging().withVersion("1.0")
                .withRetentionPolicy(disabled))
                .withDefaultServiceVersion("2018-03-28")).blockingGet()
    }

    def "List containers"() {
        setup:
        ServiceListContainersSegmentResponse response =
                primaryServiceURL.listContainersSegment(null, new ListContainersOptions(null,
                        containerPrefix, null)).blockingGet()

        expect:
        for (ContainerItem c : response.body().containerItems()) {
            c.name().startsWith(containerPrefix)
        }
        response.headers().requestId() != null
        response.headers().version() != null
    }

    def "List containers marker"() {
        setup:
        for (int i = 0; i < 10; i++) {
            ContainerURL cu = primaryServiceURL.createContainerURL(generateContainerName())
            cu.create(null, null).blockingGet()
        }

        ServiceListContainersSegmentResponse response =
                primaryServiceURL.listContainersSegment(null,
                        new ListContainersOptions(null, null, 5)).blockingGet()
        String marker = response.body().nextMarker()
        String firstContainerName = response.body().containerItems().get(0).name()
        response = primaryServiceURL.listContainersSegment(marker,
                new ListContainersOptions(null, null, 5)).blockingGet()

        expect:
        // Assert that the second segment is indeed after the first alphabetically
        firstContainerName < response.body().containerItems().get(0).name()
    }

    def "List containers details"() {
        setup:
        Metadata metadata = new Metadata()
        metadata.put("foo", "bar")
        cu = primaryServiceURL.createContainerURL("aaa" + generateContainerName())
        cu.create(metadata, null).blockingGet()

        expect:
        primaryServiceURL.listContainersSegment(null,
                new ListContainersOptions(new ContainerListingDetails(true),
                        "aaa" + containerPrefix, null)).blockingGet().body().containerItems()
                .get(0).metadata() == metadata
        // Container with prefix "aaa" will not be cleaned up by normal test cleanup.
        cu.delete(null).blockingGet().statusCode() == 202
    }

    def "List containers maxResults"() {
        setup:
        for (int i = 0; i < 11; i++) {
            primaryServiceURL.createContainerURL(generateContainerName()).create(null, null)
                    .blockingGet()
        }
        expect:
        primaryServiceURL.listContainersSegment(null,
                new ListContainersOptions(null, null, 10))
                .blockingGet().body().containerItems().size() == 10
    }

    def "List containers error"() {
        when:
        primaryServiceURL.listContainersSegment("garbage", null).blockingGet()

        then:
        thrown(StorageException)
    }

    def validatePropsSet(StorageServiceProperties sent, StorageServiceProperties received) {
        return received.logging().read() == sent.logging().read() &&
                received.logging().delete() == sent.logging().delete() &&
                received.logging().write() == sent.logging().write() &&
                received.logging().version() == sent.logging().version() &&
                received.logging().retentionPolicy().days() == sent.logging().retentionPolicy().days() &&
                received.logging().retentionPolicy().enabled() == sent.logging().retentionPolicy().enabled() &&

                received.cors().size() == sent.cors().size() &&
                received.cors().get(0).allowedMethods() == sent.cors().get(0).allowedMethods() &&
                received.cors().get(0).allowedHeaders() == sent.cors().get(0).allowedHeaders() &&
                received.cors().get(0).allowedOrigins() == sent.cors().get(0).allowedOrigins() &&
                received.cors().get(0).exposedHeaders() == sent.cors().get(0).exposedHeaders() &&
                received.cors().get(0).maxAgeInSeconds() == sent.cors().get(0).maxAgeInSeconds() &&

                received.defaultServiceVersion() == sent.defaultServiceVersion() &&

                received.hourMetrics().enabled() == sent.hourMetrics().enabled() &&
                received.hourMetrics().includeAPIs() == sent.hourMetrics().includeAPIs() &&
                received.hourMetrics().retentionPolicy().enabled() == sent.hourMetrics().retentionPolicy().enabled() &&
                received.hourMetrics().retentionPolicy().days() == sent.hourMetrics().retentionPolicy().days() &&
                received.hourMetrics().version() == sent.hourMetrics().version() &&

                received.minuteMetrics().enabled() == sent.minuteMetrics().enabled() &&
                received.minuteMetrics().includeAPIs() == sent.minuteMetrics().includeAPIs() &&
                received.minuteMetrics().retentionPolicy().enabled() == sent.minuteMetrics().retentionPolicy().enabled() &&
                received.minuteMetrics().retentionPolicy().days() == sent.minuteMetrics().retentionPolicy().days() &&
                received.minuteMetrics().version() == sent.minuteMetrics().version() &&

                received.deleteRetentionPolicy().enabled() == sent.deleteRetentionPolicy().enabled() &&
                received.deleteRetentionPolicy().days() == sent.deleteRetentionPolicy().days() &&

                received.staticWebsite().enabled() == sent.staticWebsite().enabled() &&
                received.staticWebsite().indexDocument() == sent.staticWebsite().indexDocument() &&
                received.staticWebsite().errorDocument404Path() == sent.staticWebsite().errorDocument404Path()
    }

    def "Set get properties"() {
        when:
        RetentionPolicy retentionPolicy = new RetentionPolicy().withDays(5).withEnabled(true)
        Logging logging = new Logging().withRead(true).withVersion("1.0")
                .withRetentionPolicy(retentionPolicy)
        ArrayList<CorsRule> corsRules = new ArrayList<>()
        corsRules.add(new CorsRule().withAllowedMethods("GET,PUT,HEAD")
                .withAllowedOrigins("*")
                .withAllowedHeaders("x-ms-version")
                .withExposedHeaders("x-ms-client-request-id")
                .withMaxAgeInSeconds(10))
        String defaultServiceVersion = "2016-05-31"
        Metrics hourMetrics = new Metrics().withEnabled(true).withVersion("1.0")
                .withRetentionPolicy(retentionPolicy).withIncludeAPIs(true)
        Metrics minuteMetrics = new Metrics().withEnabled(true).withVersion("1.0")
                .withRetentionPolicy(retentionPolicy).withIncludeAPIs(true)
        StaticWebsite website = new StaticWebsite().withEnabled(true)
                .withIndexDocument("myIndex.html")
                .withErrorDocument404Path("custom/error/path.html")

        StorageServiceProperties sentProperties = new StorageServiceProperties()
                .withLogging(logging).withCors(corsRules).withDefaultServiceVersion(defaultServiceVersion)
                .withMinuteMetrics(minuteMetrics).withHourMetrics(hourMetrics)
                .withDeleteRetentionPolicy(retentionPolicy)
                .withStaticWebsite(website)

        ServiceSetPropertiesHeaders headers = primaryServiceURL.setProperties(sentProperties)
                .blockingGet().headers()

        // Service properties may take up to 30s to take effect. If they weren't already in place, wait.
        sleep(30 * 1000)

        StorageServiceProperties receivedProperties = primaryServiceURL.getProperties()
                .blockingGet().body()

        then:
        headers.requestId() != null
        headers.version() != null
        validatePropsSet(sentProperties, receivedProperties)
    }

    // In java, we don't have support from the validator for checking the bounds on days. The service will catch these.

    def "Set props error"() {
        when:
        new ServiceURL(new URL("https://error.blob.core.windows.net"),
                StorageURL.createPipeline(primaryCreds, new PipelineOptions()))
                .setProperties(new StorageServiceProperties()).blockingGet()

        then:
        thrown(StorageException)
    }

    def "Get props error"() {
        when:
        new ServiceURL(new URL("https://error.blob.core.windows.net"),
                StorageURL.createPipeline(primaryCreds, new PipelineOptions())).getProperties().blockingGet()

        then:
        thrown(StorageException)
    }

    def "Get stats"() {
        setup:
        BlobURLParts parts = URLParser.parse(primaryServiceURL.toURL())
        parts.host = "xclientdev3-secondary.blob.core.windows.net"
        ServiceURL secondary = new ServiceURL(parts.toURL(),
                StorageURL.createPipeline(primaryCreds, new PipelineOptions()))
        ServiceGetStatisticsResponse response = secondary.getStatistics().blockingGet()

        expect:
        response.headers().version() != null
        response.headers().requestId() != null
        response.headers().date() != null
        response.body().geoReplication().status() != null
        response.body().geoReplication().lastSyncTime() != null
    }

    def "Get stats error"() {
        when:
        primaryServiceURL.getStatistics().blockingGet()

        then:
        thrown(StorageException)
    }
}
