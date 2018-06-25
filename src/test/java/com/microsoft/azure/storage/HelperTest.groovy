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

import com.microsoft.azure.storage.blob.RequestRetryFactory
import com.microsoft.azure.storage.blob.RequestRetryOptions
import com.microsoft.azure.storage.blob.RetryPolicyType
import com.microsoft.azure.storage.blob.StorageException
import com.microsoft.azure.storage.blob.models.StorageErrorCode
import com.microsoft.azure.storage.blob.models.StorageErrorException
import com.microsoft.rest.v2.http.HttpHeaders
import com.microsoft.rest.v2.http.HttpMethod
import com.microsoft.rest.v2.http.HttpPipeline
import com.microsoft.rest.v2.http.HttpRequest
import com.microsoft.rest.v2.http.HttpResponse
import io.reactivex.Flowable
import spock.lang.Unroll

class HelperTest extends APISpec {

    static URL retryTestURL = new URL("http://" + RequestRetryTestFactory.RETRY_TEST_PRIMARY_HOST)

    // tryTimeout must be greater than maxRetryDelayInMs. 1 4 5
    static RequestRetryOptions retryTestOptions = new RequestRetryOptions(RetryPolicyType.EXPONENTIAL, 6,
            5, 1000, 4000, RequestRetryTestFactory.RETRY_TEST_SECONDARY_HOST)

    def "responseError"() {
        when:
        cu.listBlobsFlatSegment("garbage", null).blockingGet()

        then:
        def e = thrown(StorageException)
        e.errorCode() == StorageErrorCode.INVALID_QUERY_PARAMETER_VALUE
        e.statusCode() == 400
        e.message().contains("Value for one of the query parameters specified in the request URI is invalid.")
        e.getMessage().contains("<?xml") // Ensure that the details in the payload are printable
    }

    def "Retries until success"() {
        setup:
        RequestRetryTestFactory retryTestFactory = new RequestRetryTestFactory(
                RequestRetryTestFactory.RETRY_TEST_SCENARIO_RETRY_UNTIL_SUCCESS, retryTestOptions)
        HttpPipeline pipeline = HttpPipeline.build(new RequestRetryFactory(retryTestOptions), retryTestFactory)


        when:
        HttpResponse response = pipeline.sendRequestAsync(new HttpRequest(null, HttpMethod.GET,
                retryTestURL, new HttpHeaders(),
                Flowable.just(RequestRetryTestFactory.RETRY_TEST_DEFAULT_DATA), null)).blockingGet()

        then:
        response.statusCode() == 200
        retryTestFactory.getTryNumber() == 6
    }

    def "Retries until max retries"() {
        setup:
        RequestRetryTestFactory retryTestFactory = new RequestRetryTestFactory(
                RequestRetryTestFactory.RETRY_TEST_SCENARIO_RETRY_UNTIL_MAX_RETRIES, retryTestOptions)
        HttpPipeline pipeline = HttpPipeline.build(new RequestRetryFactory(retryTestOptions), retryTestFactory)

        when:
        pipeline.sendRequestAsync(new HttpRequest(null, HttpMethod.GET, retryTestURL, new HttpHeaders(),
                Flowable.just(RequestRetryTestFactory.RETRY_TEST_DEFAULT_DATA), null)).blockingGet()

        then:
        def e = thrown(StorageErrorException)
        e.response().statusCode() == 503
        retryTestFactory.tryNumber == retryTestOptions.maxTries
    }

    def "Retries non retryable"() {
        setup:
        RequestRetryTestFactory retryTestFactory = new RequestRetryTestFactory(
                RequestRetryTestFactory.RETRY_TEST_SCENARIO_NON_RETRYABLE, retryTestOptions)
        HttpPipeline pipeline = HttpPipeline.build(new RequestRetryFactory(retryTestOptions), retryTestFactory)

        when:
        pipeline.sendRequestAsync(new HttpRequest(null, HttpMethod.GET, retryTestURL, new HttpHeaders(),
                Flowable.just(RequestRetryTestFactory.RETRY_TEST_DEFAULT_DATA), null)).blockingGet()

        then:
        def e = thrown(StorageErrorException)
        e.response().statusCode() == 400
        retryTestFactory.tryNumber == 1
    }

    def "Retries non retryable secondary"() {
        setup:
        RequestRetryTestFactory retryTestFactory = new RequestRetryTestFactory(
                RequestRetryTestFactory.RETRY_TEST_SCENARIO_NON_RETRYABLE_SECONDARY, retryTestOptions)
        HttpPipeline pipeline = HttpPipeline.build(new RequestRetryFactory(retryTestOptions), retryTestFactory)

        when:
        pipeline.sendRequestAsync(new HttpRequest(null, HttpMethod.GET, retryTestURL, new HttpHeaders(),
                Flowable.just(RequestRetryTestFactory.RETRY_TEST_DEFAULT_DATA), null)).blockingGet()

        then:
        def e = thrown(StorageErrorException)
        e.response().statusCode() == 400
        retryTestFactory.tryNumber == 2
    }

    def "Retries network error"() {
        setup:
        RequestRetryTestFactory retryTestFactory = new RequestRetryTestFactory(
                RequestRetryTestFactory.RETRY_TEST_SCENARIO_NETWORK_ERROR, retryTestOptions)
        HttpPipeline pipeline = HttpPipeline.build(new RequestRetryFactory(retryTestOptions), retryTestFactory)

        when:
        HttpResponse response =
                pipeline.sendRequestAsync(new HttpRequest(null, HttpMethod.GET, retryTestURL,
                        new HttpHeaders(), Flowable.just(RequestRetryTestFactory.RETRY_TEST_DEFAULT_DATA),
                        null)).blockingGet()

        then:
        response.statusCode() == 200
        retryTestFactory.tryNumber == 2
    }

    def "Retries try timeout"() {
        setup:
        RequestRetryTestFactory retryTestFactory = new RequestRetryTestFactory(
                RequestRetryTestFactory.RETRY_TEST_SCENARIO_TRY_TIMEOUT, retryTestOptions)
        HttpPipeline pipeline = HttpPipeline.build(new RequestRetryFactory(retryTestOptions), retryTestFactory)

        when:
        HttpResponse response =
                pipeline.sendRequestAsync(new HttpRequest(null, HttpMethod.GET, retryTestURL,
                        new HttpHeaders(), Flowable.just(RequestRetryTestFactory.RETRY_TEST_DEFAULT_DATA),
                        null)).blockingGet()

        then:
        response.statusCode() == 200
        retryTestFactory.tryNumber == 3
    }

    def "Retries exponential delay"() {
        setup:
        RequestRetryTestFactory retryTestFactory = new RequestRetryTestFactory(
                RequestRetryTestFactory.RETRY_TEST_SCENARIO_EXPONENTIAL_TIMING, retryTestOptions)
        HttpPipeline pipeline = HttpPipeline.build(new RequestRetryFactory(retryTestOptions), retryTestFactory)

        when:
        HttpResponse response =
                pipeline.sendRequestAsync(new HttpRequest(null, HttpMethod.GET, retryTestURL,
                        new HttpHeaders(), Flowable.just(RequestRetryTestFactory.RETRY_TEST_DEFAULT_DATA),
                        null)).blockingGet()

        then:
        response.statusCode() == 200
        retryTestFactory.tryNumber == 6
    }

    def "Retries fixed delay"() {
        setup:
        RequestRetryTestFactory retryTestFactory = new RequestRetryTestFactory(
                RequestRetryTestFactory.RETRY_TEST_SCENARIO_FIXED_TIMING, retryTestOptions)
        HttpPipeline pipeline = HttpPipeline.build(new RequestRetryFactory(retryTestOptions), retryTestFactory)

        when:
        HttpResponse response =
                pipeline.sendRequestAsync(new HttpRequest(null, HttpMethod.GET, retryTestURL,
                        new HttpHeaders(), Flowable.just(RequestRetryTestFactory.RETRY_TEST_DEFAULT_DATA),
                        null)).blockingGet()

        then:
        response.statusCode() == 200
        retryTestFactory.tryNumber == 4
    }

    @Unroll
    def "Retries options invalid"() {
        when:
        new RequestRetryOptions(null, maxTries, tryTimeout,
                retryDelayInMs, maxRetryDelayInMs, null)

        then:
        thrown(IllegalArgumentException)

        where:
        maxTries | tryTimeout | retryDelayInMs | maxRetryDelayInMs
        0        | null       | null           | null
        null     | 0          | null           | null
        null     | null       | 0              | 1
        null     | null       | 1              | 0
        null     | null       | null           | 1
        null     | null       | 1              | null
        null     | null       | 5              | 4
    }
}
