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

import com.microsoft.azure.storage.file.*
import com.microsoft.azure.storage.file.models.CopyStatusType
import com.microsoft.azure.storage.file.models.ShareItem
import com.microsoft.rest.v2.Context
import com.microsoft.rest.v2.http.*
import com.microsoft.rest.v2.policy.RequestPolicy
import com.microsoft.rest.v2.policy.RequestPolicyFactory
import io.reactivex.Flowable
import io.reactivex.Single
import org.spockframework.lang.ISpecificationContext
import spock.lang.Shared
import spock.lang.Specification

import java.nio.ByteBuffer
import java.time.OffsetDateTime

class APISpec extends Specification {
    @Shared
    Integer iterationNo = 0 // Used to generate stable share names for recording tests with multiple iterations.

    Integer entityNo = 0 // Used to generate stable share names for recording tests requiring multiple shares.

    @Shared
    ServiceURL su
    ShareURL shu

    // Fields used for conveniently creating files with data.
    static final String defaultText = "default"

    static final ByteBuffer defaultData = ByteBuffer.wrap(defaultText.bytes)

    static final Flowable<ByteBuffer> defaultFlowable = Flowable.just(defaultData)

    static defaultDataSize = defaultData.remaining()

    // If debugging is enabled, recordings cannot run as there can only be one proxy at a time.
    static boolean enableDebugging = false

    // Prefixes for shares and files
    static String sharePrefix = "jts" // java test share

    static String filePrefix = "javafile"

    static String directoryPrefix = "javadirectory"

    static long defaultFileSize = defaultText.size()

    /*
    Credentials for various kinds of accounts.
     */
    static SharedKeyCredentials primaryCreds = getGenericCreds("")

    static ServiceURL primaryServiceURL = getGenericServiceURL(primaryCreds)

    static SharedKeyCredentials alternateCreds = getGenericCreds("SECONDARY_")

    static ServiceURL alternateServiceURL = getGenericServiceURL(alternateCreds)

    /*
    Constants for testing that the context parameter is properly passed to the pipeline.
     */
    static final String defaultContextKey = "Key"

    static final String defaultContextValue = "Value"

    static final Context defaultContext = new Context(defaultContextKey, defaultContextValue)

    static String getTestName(ISpecificationContext ctx) {
        return ctx.getCurrentFeature().name.replace(' ', '').toLowerCase()
    }

    def generateShareName() {
        generateShareName(specificationContext, iterationNo, entityNo++)
    }

    def generateFileName() {
        generateFileName(specificationContext, iterationNo, entityNo++)
    }

    def generateDirectoryName() {
        generateDirectoryName(specificationContext, iterationNo, entityNo++)
    }

    /**
     * This function generates an entity name by concatenating the passed prefix, the name of the test requesting the
     * entity name, and some unique suffix. This ensures that the entity name is unique for each test so there are
     * no conflicts on the service. If we are not recording, we can just use the time. If we are recording, the suffix
     * must always be the same so we can match requests. To solve this, we use the entityNo for how many entities have
     * already been created by this test so far. This would sufficiently distinguish entities within a recording, but
     * could still yield duplicates on the service for data-driven tests. Therefore, we also add the iteration number
     * of the data driven tests.
     *
     * @param specificationContext
     *      Used to obtain the name of the test running.
     * @param prefix
     *      Used to group all entities created by these tests under common prefixes. Useful for listing.
     * @param iterationNo
     *      Indicates which iteration of a data-driven test is being executed.
     * @param entityNo
     *      Indicates how man entities have been created by the test so far. This distinguishes multiple shares
     *      or multiple files created by the same test. Only used when dealing with recordings.
     * @return
     */
    static String generateResourceName(ISpecificationContext specificationContext, String prefix, int iterationNo,
                                       int entityNo) {
        String suffix = ""
        suffix += System.currentTimeMillis() // For uniqueness between runs.
        suffix += entityNo // For easy identification of which call created this resource.
        return prefix + getTestName(specificationContext) + suffix
    }

    static int updateIterationNo(ISpecificationContext specificationContext, int iterationNo) {
        if (specificationContext.currentIteration.estimatedNumIterations > 1) {
            return iterationNo + 1
        } else {
            return 0
        }
    }

    static String generateShareName(ISpecificationContext specificationContext, int iterationNo, int entityNo) {
        return generateResourceName(specificationContext, sharePrefix, iterationNo, entityNo)
    }

    static String generateFileName(ISpecificationContext specificationContext, int iterationNo, int entityNo) {
        return generateResourceName(specificationContext, filePrefix, iterationNo, entityNo)
    }

    static String generateDirectoryName(ISpecificationContext specificationContext, int iterationNo, int entityNo) {
        return generateResourceName(specificationContext, directoryPrefix, iterationNo, entityNo)
    }

    static void setupFeatureRecording(String sceneName) {

    }

    static void scrubAuthHeader(String sceneName) {

    }

    static getGenericCreds(String accountType) {
        return new SharedKeyCredentials(System.getenv().get(accountType + "ACCOUNT_NAME"),
                System.getenv().get(accountType + "ACCOUNT_KEY"))
    }

    static HttpClient getHttpClient() {
        if (enableDebugging) {
            HttpClientConfiguration configuration = new HttpClientConfiguration(
                    new Proxy(Proxy.Type.HTTP, new InetSocketAddress("localhost", 8888)))
            return HttpClient.createDefault(configuration)
        } else return HttpClient.createDefault()
    }

    static ServiceURL getGenericServiceURL(SharedKeyCredentials creds) {
        PipelineOptions po = new PipelineOptions()
        po.withClient(getHttpClient())

        HttpPipeline pipeline = StorageURL.createPipeline(creds, po)

        return new ServiceURL(new URL("http://" + creds.getAccountName() + ".file.core.windows.net"), pipeline)
    }

    static void cleanupShares() throws MalformedURLException {
        // We don't need to clean up shares if we are playing back
        // Create a new pipeline without any proxies
        HttpPipeline pipeline = StorageURL.createPipeline(primaryCreds, new PipelineOptions())

        ServiceURL serviceURL = new ServiceURL(
                new URL("http://" + System.getenv().get("ACCOUNT_NAME") + ".file.core.windows.net"), pipeline)
        // There should not be more than 5000 shares from these tests
        for (ShareItem s : serviceURL.listSharesSegment(null,
                new ListSharesOptions().withPrefix(sharePrefix), null).blockingGet()
                .body().shareItems()) {
            ShareURL shareUrl = serviceURL.createShareURL(s.name())
            shareUrl.delete(null, null).blockingGet()
        }
    }

    static ByteBuffer getRandomData(long size) {
        Random rand = new Random(getRandomSeed())
        byte[] data = new byte[size]
        rand.nextBytes(data)
        return ByteBuffer.wrap(data)
    }

    static File getRandomFile(long size) {
        File file = File.createTempFile(UUID.randomUUID().toString(), ".txt")
        file.deleteOnExit()
        FileOutputStream fos = new FileOutputStream(file)
        fos.write(getRandomData(size).array())
        fos.close()
        return file
    }

    static long getRandomSeed() {
        return System.currentTimeMillis()
    }

    def setupSpec() {
    }

    def cleanupSpec() {
        cleanupShares()
    }

    def setup() {
        su = primaryServiceURL
        shu = primaryServiceURL.createShareURL(generateShareName())
        shu.create(null, null, null).blockingGet()
    }

    def cleanup() {
        // TODO: Scrub auth header here?
        iterationNo = updateIterationNo(specificationContext, iterationNo)
    }

    def waitForCopy(FileURL fu, CopyStatusType status) {
        OffsetDateTime start = OffsetDateTime.now()
        while (status != CopyStatusType.SUCCESS) {
            status = fu.getProperties(null).blockingGet().headers().copyStatus()
            OffsetDateTime currentTime = OffsetDateTime.now()
            if (status == CopyStatusType.FAILED || currentTime.minusMinutes(1) == start) {
                throw new Exception("Copy failed or took too long")
            }
            sleep(1000)
        }
    }

    /**
     * Validates the presence of headers that are present on large number of responses. These headers are generally
     * random and can really only be checked as not null.
     * @param headers
     *      The object (may be headers object or response object) that has properties which expose these common headers.
     * @return
     * Whether or not the header values are appropriate.
     */
    def validateBasicHeaders(Object headers) {
        return headers.class.getMethod("requestId").invoke(headers) != null &&
                headers.class.getMethod("version").invoke(headers) != null &&
                headers.class.getMethod("date").invoke(headers) != null
    }

    /**
     * Validates the presence of headers that are present on a large number responses. These headers are generally
     * random and can really only be checked as not null.
     * @param headers
     *      The object (may be headers object or response object) that has properties which expose these common headers.
     * @return
     * Whether or not the header values are appropriate.
     */
    def validateResponseHeaders(Object headers) {
        return headers.class.getMethod("eTag").invoke(headers) != null &&
                headers.class.getMethod("lastModified").invoke(headers) != null &&
                headers.class.getMethod("requestId").invoke(headers) != null &&
                headers.class.getMethod("version").invoke(headers) != null &&
                headers.class.getMethod("date").invoke(headers) != null
    }

    def validateFileHeaders(Object headers, String cacheControl, String contentDisposition, String contentEncoding,
                            String contentLangauge, byte[] contentMD5, String contentType) {
        return headers.class.getMethod("cacheControl").invoke(headers) == cacheControl &&
                headers.class.getMethod("contentDisposition").invoke(headers) == contentDisposition &&
                headers.class.getMethod("contentEncoding").invoke(headers) == contentEncoding &&
                headers.class.getMethod("contentLanguage").invoke(headers) == contentLangauge &&
                headers.class.getMethod("contentMD5").invoke(headers) == contentMD5 &&
                headers.class.getMethod("contentType").invoke(headers) == contentType

    }

    /*
    This method returns a stub of an HttpResponse. This is for when we want to test policies in isolation but don't care
     about the status code, so we stub a response that always returns a given value for the status code. We never care
     about the number or nature of interactions with this stub.
     */

    def getStubResponse(int code) {
        return Stub(HttpResponse) {
            statusCode() >> code
        }
    }

    /*
    This is for stubbing responses that will actually go through the pipeline and autorest code. Autorest does not seem
    to play too nicely with mocked objects and the complex reflection stuff on both ends made it more difficult to work
    with than was worth it.
     */

    def getStubResponse(int code, Class responseHeadersType) {
        return new HttpResponse() {

            @Override
            int statusCode() {
                return code
            }

            @Override
            String headerValue(String s) {
                return null
            }

            @Override
            HttpHeaders headers() {
                return new HttpHeaders()
            }

            @Override
            Flowable<ByteBuffer> body() {
                return Flowable.empty()
            }

            @Override
            Single<byte[]> bodyAsByteArray() {
                return null
            }

            @Override
            Single<String> bodyAsString() {
                return null
            }

            @Override
            Object deserializedHeaders() {
                return responseHeadersType.getConstructor().newInstance()
            }

            @Override
            boolean isDecoded() {
                return true
            }
        }
    }

    def getContextStubPolicy(int successCode, Class responseHeadersType) {
        return Mock(RequestPolicy) {
            sendAsync(_) >> { HttpRequest request ->
                if (!request.context().getData(defaultContextKey).isPresent()) {
                    return Single.error(new RuntimeException("Context key not present."))
                } else {
                    return Single.just(getStubResponse(successCode, responseHeadersType))
                }
            }
        }
    }

    def getStubFactory(RequestPolicy policy) {
        return Mock(RequestPolicyFactory) {
            create(*_) >> policy
        }
    }
}
