package com.microsoft.azure.storage

import com.microsoft.azure.storage.file.*
import com.microsoft.azure.storage.file.models.*
import com.microsoft.rest.v2.http.HttpPipeline
import com.microsoft.rest.v2.util.FlowableUtil
import io.reactivex.Flowable
import spock.lang.Unroll

import java.nio.ByteBuffer
import java.security.MessageDigest
import java.time.OffsetDateTime

class FileAPITest extends APISpec{
    DirectoryURL du
    FileURL fu

    def setup() {
        du = shu.createDirectoryURL(generateDirectoryName())
        du.create(null, null).blockingGet()
        fu = du.createFileURL(generateFileName())
    }

    def "File create"(){
        when:
        FileCreateResponse response = fu.create(10, null, null, null).blockingGet()

        then:
        response.statusCode() == 201
        validateResponseHeaders(response.headers())
    }

    def "File create metadata"(){
        setup:
        Metadata metadata = new Metadata()
        metadata.put("Key1", "Value1")
        metadata.put("Key2", "Value2")
        FileCreateResponse createResponse = fu.create(10, null, metadata, null).blockingGet()

        when:
        FileGetPropertiesResponse propertiesResponse = fu.getProperties(null).blockingGet()

        then:
        createResponse.statusCode() == 201
        validateResponseHeaders(createResponse.headers())
        propertiesResponse.statusCode() == 200
        validateResponseHeaders(propertiesResponse.headers())
        propertiesResponse.headers().metadata() == metadata
    }

    def "File create http headers"(){
        setup:
        def basicHeader = new FileHTTPHeaders().withFileContentType(contentType)
                                                .withFileContentDisposition(contentDisposition)
                                                .withFileCacheControl(cacheControl)
                                                .withFileContentMD5(contentMD5)
                                                .withFileContentLanguage(contentLanguage)
                                                .withFileContentEncoding(contentEncoding)

        FileCreateResponse createResponse = fu.create(10, basicHeader,null, null).blockingGet()

        when:
        FileGetPropertiesResponse propertiesResponse = fu.getProperties(null).blockingGet()

        then:
        createResponse.statusCode() == 201
        validateResponseHeaders(createResponse.headers())
        propertiesResponse.statusCode() == 200
        validateResponseHeaders(propertiesResponse.headers())
        propertiesResponse.headers().contentType() == (contentType == null ? "application/octet-stream" : contentType)
        propertiesResponse.headers().contentDisposition() == contentDisposition
        propertiesResponse.headers().cacheControl() == cacheControl
        propertiesResponse.headers().contentMD5() == contentMD5
        propertiesResponse.headers().contentLanguage() == contentLanguage
        propertiesResponse.headers().contentEncoding() == contentEncoding

        where:
        contentType | contentDisposition    | cacheControl  | contentMD5                                                                                    | contentLanguage   | contentEncoding
        "my-type"   | null                  | null          |    null                                                                                       | null              | null
        "my-type"   | "my_disposition"      | "my_cache"    | MessageDigest.getInstance("MD5").digest("md5String".getBytes("UTF-8"))   | "my_language"     | "my_encoding"
    }

    def "File create error"(){
        setup:
        du = shu.createDirectoryURL(generateDirectoryName())
        fu = du.createFileURL(generateFileName())

        when:
        fu.create(10, null, null, null).blockingGet()

        then:
        def e = thrown(StorageException)
        e.statusCode() == 404
        e.errorCode() == StorageErrorCode.PARENT_NOT_FOUND
    }

    def "File create context"() {
        setup:
        def pipeline = HttpPipeline.build(getStubFactory(getContextStubPolicy(201, FileCreateHeaders)))
        fu.withPipeline(pipeline)

        when:
        // No service call is made. Just satisfy the parameters.
        fu.create(10, null, null, defaultContext).blockingGet()

        then:
        notThrown(RuntimeException)
    }

    def "File upload range"(){
        setup:
        fu.create(defaultFileSize, null, null, null).blockingGet()

        when:
        def range = new FileRange().withOffset(0).withCount(defaultFileSize)
        FileUploadRangeResponse response = fu.uploadRange(range, Flowable.just(defaultData), null).blockingGet()

        then:
        validateResponseHeaders(response.headers())
        response.statusCode() == 201
    }

    def "File upload range negative offset"(){
        setup:
        fu.create(defaultFileSize, null, null, null).blockingGet()

        when:
        def range = new FileRange().withOffset(-2).withCount(defaultFileSize)
        FileUploadRangeResponse response = fu.uploadRange(range, Flowable.just(defaultData), null).blockingGet()

        then:
        thrown(IllegalArgumentException)
    }

    def "File upload range empty body"(){
        setup:
        fu.create(defaultFileSize, null, null, null).blockingGet()

        when:
        def range = new FileRange().withOffset(0).withCount(0)
        FileUploadRangeResponse response = fu.uploadRange(range, Flowable.just(ByteBuffer.wrap(new byte[0])), null).blockingGet()

        then:
        def e = thrown(StorageException)
        e.statusCode() == 400
        e.errorCode() == StorageErrorCode.INVALID_HEADER_VALUE
    }

    def "File upload range null body"(){
        setup:
        fu.create(defaultFileSize, null, null, null).blockingGet()

        when:
        FileUploadRangeResponse response = fu.uploadRange(null, Flowable.just(null), null).blockingGet()

        then:
        thrown(NullPointerException)
    }

    def "File upload range error"(){
        when:
        fu.uploadRange(new FileRange().withOffset(0).withCount(defaultFileSize), Flowable.just(defaultData), null).blockingGet()

        then:
        def e = thrown(StorageException)
        e.statusCode() == 404
        e.errorCode() == StorageErrorCode.RESOURCE_NOT_FOUND
    }

    def "File uploadRange context"() {
        setup:
        def pipeline = HttpPipeline.build(getStubFactory(getContextStubPolicy(201, FileUploadRangeHeaders)))
        fu.withPipeline(pipeline)

        when:
        // No service call is made. Just satisfy the parameters.
        fu.create(defaultFileSize, null, null, defaultContext).blockingGet()

        then:
        notThrown(RuntimeException)
    }

    def "File clear all range"(){
        setup:
        fu.create(512, null, null, null).blockingGet()
        def range = new FileRange().withOffset(0).withCount(512)
        fu.uploadRange(range, Flowable.just(getRandomData(512)), null).blockingGet()

        when:
        FileUploadRangeResponse response = fu.clearRange(range, null).blockingGet()
        FileGetRangeListResponse rangeListResponse = fu.getRangeList(FileRange.DEFAULT, null).blockingGet()

        then:
        validateResponseHeaders(response.headers())
        response.statusCode() == 201
        rangeListResponse.body().size() == 0
    }

    def "File clear some range"(){
        setup:
        fu.create(2048*2, null, null, null).blockingGet()
        fu.uploadRange(new FileRange().withOffset(0).withCount(1024), Flowable.just(getRandomData(1024)), null).blockingGet()
        fu.uploadRange(new FileRange().withOffset(1024).withCount(1024), Flowable.just(getRandomData(1024)), null).blockingGet()
        fu.uploadRange(new FileRange().withOffset(3072).withCount(1024), Flowable.just(getRandomData(1024)), null).blockingGet()

        when:
        fu.clearRange(new FileRange().withOffset(0).withCount(1024), null).blockingGet()
        FileGetRangeListResponse response = fu.getRangeList(new FileRange().withOffset(0).withCount(0), null).blockingGet()

        then:
        validateResponseHeaders(response.headers())
        response.statusCode() == 200
        response.body().size() == 2
        assert (response.body().get(0).start() == 1024 && response.body().get(0).end()==2047)
        assert (response.body().get(1).start() == 3072 && response.body().get(1).end()==4095)
    }

    def "File unaligned clear range"(){
        setup:
        fu.create(1, null, null, null).blockingGet()
        fu.uploadRange(new FileRange().withOffset(0).withCount(1), Flowable.just(getRandomData(1)), null).blockingGet()

        when:
        fu.clearRange(new FileRange().withOffset(0).withCount(1), null).blockingGet()
        FileGetRangeListResponse response = fu.getRangeList(new FileRange().withOffset(0).withCount(0), null).blockingGet()

        then:
        validateResponseHeaders(response.headers())
        response.statusCode() == 200
        response.body().size() == 1
    }

    def "File clear range negative offset"(){
        setup:
        fu.create(1, null, null, null).blockingGet()
        fu.uploadRange(new FileRange().withOffset(0).withCount(1), Flowable.just(getRandomData(1)), null).blockingGet()

        when:
        fu.clearRange(new FileRange().withOffset(-1).withCount(1), null).blockingGet()

        then:
        thrown(IllegalArgumentException)
    }

    def "File clear range error"(){
        when:
        fu.clearRange(new FileRange().withOffset(0).withCount(defaultFileSize),null).blockingGet()

        then:
        def e = thrown(StorageException)
        e.statusCode() == 404
        e.errorCode() == StorageErrorCode.RESOURCE_NOT_FOUND
    }

    def "Set metadata all null"() {
        when:
        fu.create(defaultFileSize, null, null, null).blockingGet()
        FileSetMetadataResponse response = fu.setMetadata(null, null).blockingGet()

        then:
        fu.getProperties(null).blockingGet().headers().metadata().size() == 0
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
        fu.create(defaultFileSize, null, null, null).blockingGet()

        expect:
        fu.setMetadata(metadata, null).blockingGet().statusCode() == statusCode
        fu.getProperties(null).blockingGet().headers().metadata() == metadata

        where:
        key1  | value1 | key2   | value2 || statusCode
        null  | null   | null   | null   || 200
        "foo" | "bar"  | "fizz" | "buzz" || 200
    }

    def "Set metadata error"() {
        when:
        fu.setMetadata(null, null).blockingGet()

        then:
        def e = thrown(StorageException)
        e.response().statusCode() == 404
        e.errorCode() == StorageErrorCode.RESOURCE_NOT_FOUND
    }

    def "Set metadata context"() {
        setup:
        def pipeline = HttpPipeline.build(getStubFactory(getContextStubPolicy(200, FileSetMetadataHeaders)))
        fu.withPipeline(pipeline)

        when:
        // No service call is made. Just satisfy the parameters.
        fu.create(defaultFileSize, null, null, defaultContext).blockingGet()

        then:
        notThrown(RuntimeException)
    }

    def "Set HTTP headers null"() {
        setup:
        fu.create(defaultFileSize, null, null, null).blockingGet()
        FileSetHTTPHeadersResponse response = fu.setHTTPHeaders(null, null).blockingGet()

        expect:
        response.statusCode() == 200
        validateResponseHeaders(response.headers())
    }

    @Unroll
    def "Set HTTP headers headers"() {
        setup:
        fu.create(defaultFileSize, null, null, null).blockingGet()
        FileHTTPHeaders putHeaders = new FileHTTPHeaders().withFileCacheControl(cacheControl)
                .withFileContentDisposition(contentDisposition)
                .withFileContentEncoding(contentEncoding)
                .withFileContentLanguage(contentLanguage)
                .withFileContentMD5(contentMD5)
                .withFileContentType(contentType)
        fu.setHTTPHeaders(putHeaders, null).blockingGet()

        FileGetPropertiesHeaders receivedHeaders =
                fu.getProperties(null).blockingGet().headers()

        expect:
        validateFileHeaders(receivedHeaders, cacheControl, contentDisposition, contentEncoding, contentLanguage,
                contentMD5, contentType)

        where:
        cacheControl | contentDisposition | contentEncoding | contentLanguage | contentMD5                                                                               | contentType
        null         | null               | null            | null            | null                                                                                     | null
        "control"    | "disposition"      | "encoding"      | "language"      | Base64.getEncoder().encode(MessageDigest.getInstance("MD5").digest(defaultData.array())) | "type"

    }

    def "Set HTTP headers error"() {
        setup:
        fu = du.createFileURL(generateFileName())

        when:
        fu.setHTTPHeaders(null,null).blockingGet()

        then:
        thrown(StorageException)
    }

    def "Set HTTP headers context"() {
        setup:
        def pipeline = HttpPipeline.build(getStubFactory(getContextStubPolicy(200, FileSetHTTPHeadersHeaders)))

        fu = fu.withPipeline(pipeline)

        when:
        fu.setHTTPHeaders(null, defaultContext).blockingGet()

        then:
        notThrown(RuntimeException)
    }

    def "Get properties all null"() {
        when:
        fu.create(defaultFileSize, null, null, null).blockingGet()
        FileGetPropertiesHeaders headers = fu.getProperties(null).blockingGet().headers()

        then:
        validateResponseHeaders(headers)
        headers.metadata().isEmpty()
        headers.fileType() == "File"
        headers.copyCompletionTime() == null // tested in "copy"
        headers.copyStatusDescription() == null // only returned when the service has errors; cannot validate.
        headers.copyId() == null // tested in "abort copy"
        headers.copyProgress() == null // tested in "copy"
        headers.copySource() == null // tested in "copy"
        headers.copyStatus() == null // tested in "copy"
        headers.contentLength() != null
        headers.contentType() != null
        headers.contentMD5() == null
        headers.contentEncoding() == null // tested in "set HTTP headers"
        headers.contentDisposition() == null // tested in "set HTTP headers"
        headers.contentLanguage() == null // tested in "set HTTP headers"
        headers.cacheControl() == null // tested in "set HTTP headers"
        headers.isServerEncrypted()
    }

    def "Get properties error"() {
        setup:
        fu = du.createFileURL(generateFileName())

        when:
        fu.getProperties(null).blockingGet()

        then:
        thrown(StorageException)
    }

    def "Get properties context"() {
        setup:
        def pipeline = HttpPipeline.build(getStubFactory(getContextStubPolicy(200, FileGetPropertiesHeaders)))

        fu = fu.withPipeline(pipeline)

        when:
        fu.getProperties(defaultContext).blockingGet()

        then:
        notThrown(RuntimeException)
    }

    def "Copy"() {
        setup:
        fu.create(defaultFileSize, null, null, null).blockingGet()
        fu.uploadRange(new FileRange().withOffset(0).withCount(defaultFileSize), Flowable.just(defaultData), null).blockingGet()
        FileURL fu2 = du.createFileURL(generateFileName())
        FileStartCopyHeaders headers =
                fu2.startCopy(fu.toURL(), null, null).blockingGet().headers()

        when:
        while (fu2.getProperties(null).blockingGet().headers().copyStatus() == CopyStatusType.PENDING) {
            sleep(1000)
        }
        FileGetPropertiesHeaders headers2 = fu2.getProperties(null).blockingGet().headers()

        then:
        headers2.copyStatus() == CopyStatusType.SUCCESS
        headers2.copyCompletionTime() != null
        headers2.copyProgress() != null
        headers2.copySource() != null
        validateBasicHeaders(headers)
        headers.copyId() != null
    }

    @Unroll
    def "Copy metadata"() {
        setup:
        fu.create(defaultFileSize, null, null, null).blockingGet()
        fu.uploadRange(new FileRange().withOffset(0).withCount(defaultFileSize), Flowable.just(defaultData), null).blockingGet()
        FileURL fu2 = du.createFileURL(generateFileName())
        Metadata metadata = new Metadata()
        if (key1 != null && value1 != null) {
            metadata.put(key1, value1)
        }
        if (key2 != null && value2 != null) {
            metadata.put(key2, value2)
        }

        FileStartCopyResponse response =
                fu2.startCopy(fu.toURL(), metadata, null).blockingGet()
        waitForCopy(fu2, response.headers().copyStatus())

        expect:
        FileGetPropertiesResponse resp = fu2.getProperties(null).blockingGet()
        resp.headers().metadata() == metadata

        where:
        key1  | value1 | key2   | value2 || statusCode
        null  | null   | null   | null   || 200
        "foo" | "bar"  | "fizz" | "buzz" || 200
    }

    def "Copy error"() {
        when:
        fu.startCopy(new URL("http://www.error.com"), null,null).blockingGet()

        then:
        thrown(StorageException)
    }

    def "Copy context"() {
        setup:
        def pipeline = HttpPipeline.build(getStubFactory(getContextStubPolicy(202, FileStartCopyHeaders)))

        fu = fu.withPipeline(pipeline)

        when:
        // No service call is made. Just satisfy the parameters.
        fu.startCopy(new URL("http://www.example.com"), null, defaultContext).blockingGet()

        then:
        notThrown(RuntimeException)
    }

    def "Abort copy"() {
        setup:
        def shareName = generateShareName()
        shu = primaryServiceURL.createShareURL(shareName)
        shu.create().blockingGet()
        def fileName = generateFileName()
        def fu = shu.createFileURL(fileName)
        // Create the Service SAS Signature
        def v = new ServiceSASSignatureValues()
        def p = new ShareSASPermission()
                .withRead(true)
                .withWrite(true)
                .withCreate(true)
                .withDelete(true)

        v.withPermissions(p.toString())
                .withStartTime(OffsetDateTime.now().minusDays(1))
                .withExpiryTime(OffsetDateTime.now().plusDays(1))
                .withShareName(shareName)
                .withFilePath(fileName)
        // Convert the fileURL into fileURL parts and add SASQueryParameter to the fileURL
        FileURLParts parts = URLParser.parse(fu.toURL())
        parts.withSasQueryParameters(v.generateSASQueryParameters(primaryCreds)).withScheme("https")
        fu = new FileURL(parts.toURL(), StorageURL.createPipeline(new AnonymousCredentials(),
                new PipelineOptions()))
        // Create a large file that takes time to copy
        fu.create(1000 * 1024 * 1024, null, null,null).blockingGet()
        fu.uploadRange(new FileRange().withOffset(0).withCount(4 * 1024 * 1024), Flowable.just(getRandomData(4 * 1024 * 1024)), null)
        fu.uploadRange(new FileRange().withOffset(4 * 1024 * 1024).withCount(4 * 1024 * 1024), Flowable.just(getRandomData(4 * 1024 * 1024)), null)
        fu.uploadRange(new FileRange().withOffset(96 * 1024 * 1024).withCount(4 * 1024 * 1024), Flowable.just(getRandomData(4 * 1024 * 1024)), null)

        // Create a share into a secondary account, so test gets enough time to abort copy operation.
        def shu2 = alternateServiceURL.createShareURL(generateShareName())
        shu2.create(null, null, null).blockingGet()
        def fu2 = shu2.createFileURL(generateFileName())

        when:
        String copyId =
                fu2.startCopy(fu.toURL(), null, null).blockingGet().headers().copyId()
        FileAbortCopyResponse response = fu2.abortCopy(copyId,null).blockingGet()

        then:
        response.statusCode() == 204
        validateBasicHeaders(response.headers())

        cleanup:
        // Normal test cleanup will not clean up containers in the alternate account.
        fu2.delete(null).blockingGet().statusCode() == 202
        shu2.delete().blockingGet()
    }

    def "Abort copy error"() {
        when:
        fu.abortCopy("id", null).blockingGet()

        then:
        thrown(StorageException)
    }

    def "Abort copy context"() {
        setup:
        def pipeline = HttpPipeline.build(getStubFactory(getContextStubPolicy(204, FileAbortCopyHeaders)))

        fu = fu.withPipeline(pipeline)

        when:
        // No service call is made. Just satisfy the parameters.
        fu.abortCopy("id", defaultContext).blockingGet()

        then:
        notThrown(RuntimeException)
    }

    def "File download all null"() {
        setup:
        fu.create(defaultText.size(), null, null, null).blockingGet()
        fu.uploadRange(new FileRange().withOffset(0).withCount(defaultText.size()), Flowable.just(defaultData), null).blockingGet()
        when:
        DownloadResponse response = fu.download(null, false, null)
                .blockingGet()
        ByteBuffer body = FlowableUtil.collectBytesInBuffer(response.body()).blockingGet()
        FileDownloadHeaders headers = response.headers()

        then:
        validateResponseHeaders(headers)
        body == defaultData
        headers.metadata().isEmpty()
        headers.contentLength() != null
        headers.contentType() != null
        headers.contentRange() == null
        headers.contentMD5() == null
        headers.contentEncoding() == null
        headers.cacheControl() == null
        headers.contentDisposition() == null
        headers.contentLanguage() == null
        headers.copyCompletionTime() == null
        headers.copyStatusDescription() == null
        headers.copyId() == null
        headers.copyProgress() == null
        headers.copySource() == null
        headers.copyStatus() == null
        headers.acceptRanges() == "bytes"
        headers.serverEncrypted
    }

    def "File download range"() {
        setup:
        fu.create(defaultText.size(), null, null, null).blockingGet()
        fu.uploadRange(new FileRange().withOffset(0).withCount(defaultText.size()), Flowable.just(defaultData), null).blockingGet()
        FileRange range = new FileRange().withOffset(offset).withCount(count)

        when:
        ByteBuffer body = FlowableUtil.collectBytesInBuffer(
                fu.download(range, false, null).blockingGet().body()).blockingGet()
        String bodyStr = new String(body.array())

        then:
        bodyStr == expectedData

        where:
        offset | count || expectedData
        0      | null  || defaultText
        0      | 5     || defaultText.toString().substring(0, 5)
        3      | 2     || defaultText.toString().substring(3, 5)
    }

    def "File download md5"() {
        setup:
        fu.create(defaultText.size(), null, null, null).blockingGet()
        fu.uploadRange(new FileRange().withOffset(0).withCount(defaultText.size()), Flowable.just(defaultData), null).blockingGet()

        expect:
        fu.download(new FileRange().withOffset(0).withCount(3),true, null).blockingGet()
                .headers().contentMD5() ==
                MessageDigest.getInstance("MD5").digest(defaultText.substring(0, 3).getBytes())
    }

    def "File download error"() {
        setup:
        fu = shu.createFileURL(generateFileName())

        when:
        fu.download(null, false, null).blockingGet()

        then:
        thrown(StorageException)
    }

    def "file download context"() {
        setup:
        def pipeline = HttpPipeline.build(getStubFactory(getContextStubPolicy(206, FileDownloadHeaders)))

        fu = fu.withPipeline(pipeline)

        when:
        fu.download(null, false, defaultContext).blockingGet()

        then:
        notThrown(StorageException)
    }

    def "File resize increase file size"(){
        setup:
        fu.create(defaultFileSize, null, null, null).blockingGet()

        when:
        fu.resize(defaultFileSize * 2, null).blockingGet()
        FileGetPropertiesResponse response = fu.getProperties(null).blockingGet()

        then:
        response.statusCode() == 200
        validateResponseHeaders(response.headers())
        response.headers().contentLength() == defaultFileSize * 2
    }

    def "File resize zero"(){
        setup:
        fu.create(defaultFileSize, null, null, null).blockingGet()

        when:
        fu.resize(0, null).blockingGet()
        FileGetPropertiesResponse response = fu.getProperties(null).blockingGet()

        then:
        response.statusCode() == 200
        validateResponseHeaders(response.headers())
        response.headers().contentLength() == 0
    }

    def "File resize invalid size"(){
        setup:
        fu.create(defaultFileSize, null, null, null).blockingGet()

        when:
        fu.resize(-4, null).blockingGet()
        FileGetPropertiesResponse response = fu.getProperties(null).blockingGet()

        then:
        def e = thrown(StorageException)
        e.statusCode() == 400
        e.errorCode() == StorageErrorCode.INVALID_HEADER_VALUE
    }

    def "file resize context"() {
        setup:
        def pipeline = HttpPipeline.build(getStubFactory(getContextStubPolicy(200, FileSetHTTPHeadersHeaders)))

        fu = fu.withPipeline(pipeline)

        when:
        fu.resize(defaultFileSize, defaultContext).blockingGet()

        then:
        notThrown(StorageException)
    }

}
