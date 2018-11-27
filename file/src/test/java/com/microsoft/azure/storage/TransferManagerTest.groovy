package com.microsoft.azure.storage

import com.microsoft.azure.storage.file.*
import com.microsoft.azure.storage.file.models.*
import com.microsoft.rest.v2.http.HttpPipeline
import com.microsoft.rest.v2.http.HttpRequest
import com.microsoft.rest.v2.http.HttpResponse
import com.microsoft.rest.v2.policy.RequestPolicy
import com.microsoft.rest.v2.policy.RequestPolicyFactory
import com.microsoft.rest.v2.util.FlowableUtil
import io.reactivex.Flowable
import io.reactivex.Single
import io.reactivex.functions.Consumer
import spock.lang.Unroll

import java.nio.ByteBuffer
import java.nio.channels.AsynchronousFileChannel
import java.nio.file.StandardOpenOption
import java.security.MessageDigest

class TransferManagerTest extends APISpec {
    FileURL fu

    def setup() {
        fu = shu.createFileURL(generateFileName())
    }

    @Unroll
    def "Upload file single range"() {
        setup:
        def channel = AsynchronousFileChannel.open(file.toPath())
        when:
        // Range length will be ignored for single shot.
        CommonRestResponse response = TransferManager.uploadRangesToFile(channel,
                fu, 512,
                new TransferManagerUploadToFileOptions(null, null, null, 20)).blockingGet()

        then:
        responseType.isInstance(response.response()) // Ensure we did the correct type of operation.
        validateResponseHeaders(response)
        compareDataToFile(fu.download(null, false, null).blockingGet().body(), file)

        cleanup:
        channel.close()

        where:
        file                                                  || responseType
        getRandomFile(0)                                || FileCreateResponse
        getRandomFile(10)                                     || FileUploadRangeResponse // Single shot
        getRandomFile(1024 + 1) || FileUploadRangeResponse // Multi part
    }

    def compareDataToFile(Flowable<ByteBuffer> data, File file) {
        FileInputStream fis = new FileInputStream(file)

        for (ByteBuffer received : data.blockingIterable()) {
            byte[] readBuffer = new byte[received.remaining()]
            fis.read(readBuffer)
            for (int i = 0; i < received.remaining(); i++) {
                if (readBuffer[i] != received.get(i)) {
                    return false
                }
            }
        }

        fis.close()
        return true
    }

    def "Upload file illegal arguments null"() {
        when:
        TransferManager.uploadRangesToFile(file, url, 5, null).blockingGet()

        then:
        thrown(IllegalArgumentException)

        where:
        file                                                     | url
        null                                                     | new FileURL(new URL("http://account.com"), StorageURL.createPipeline(primaryCreds, new PipelineOptions()))
        AsynchronousFileChannel.open(getRandomFile(10).toPath()) | null
    }

    @Unroll
    def "Upload file illegal arguments ranges"() {
        setup:
        def channel = AsynchronousFileChannel.open(getRandomFile(fileSize).toPath())

        when:
        TransferManager.uploadRangesToFile(channel, fu,
                rangeLength, null).blockingGet()

        then:
        thrown(IllegalArgumentException)

        cleanup:
        channel.close()

        where:
        rangeLength                            | fileSize
        -1                                     | 10 // -1 is invalid.
        FileURL.FILE_MAX_RANGE_LIMIT + 1 | FileURL.FILE_MAX_RANGE_LIMIT + 10 // range length too lengthy
    }

    @Unroll
    def "Upload file headers"() {
        setup:
        // We have to use the defaultData here so we can calculate the MD5 on the uploadFile case.
        File file = File.createTempFile("testUpload", ".txt")
        file.deleteOnExit()
        if (fileSize == "small") {
            FileOutputStream fos = new FileOutputStream(file)
            fos.write(defaultData.array())
            fos.close()
        } else {
            file = getRandomFile(FileURL.FILE_MAX_RANGE_LIMIT + 10)
        }

        def channel = AsynchronousFileChannel.open(file.toPath())

        when:
        TransferManager.uploadRangesToFile(channel, fu, 1 * 1024 * 1024,
                new TransferManagerUploadToFileOptions(null, new FileHTTPHeaders()
                        .withFileCacheControl(cacheControl).withFileContentDisposition(contentDisposition)
                        .withFileContentEncoding(contentEncoding).withFileContentLanguage(contentLanguage)
                        .withFileContentMD5(contentMD5).withFileContentType(contentType), null,
                        null)).blockingGet()

        FileGetPropertiesResponse response = fu.getProperties(null).blockingGet()

        then:
        validateFileHeaders(response.headers(), cacheControl, contentDisposition, contentEncoding, contentLanguage,
                contentMD5, contentType == null ? "application/octet-stream" : contentType)

        cleanup:
        channel.close()

        where:
        fileSize | cacheControl | contentDisposition | contentEncoding | contentLanguage | contentMD5                                                   | contentType
        "small"  | null         | null               | null            | null            | null                                                         | null
        "small"  | "control"    | "disposition"      | "encoding"      | "language"      | MessageDigest.getInstance("MD5").digest(defaultData.array()) | "type"
        "large"  | null         | null               | null            | null            | null                                                         | null
        "large"  | "control"    | "disposition"      | "encoding"      | "language"      | MessageDigest.getInstance("MD5").digest(defaultData.array()) | "type"
    }

    @Unroll
    def "Upload file metadata"() {
        setup:
        Metadata metadata = new Metadata()
        if (key1 != null) {
            metadata.put(key1, value1)
        }
        if (key2 != null) {
            metadata.put(key2, value2)
        }
        def channel = AsynchronousFileChannel.open(getRandomFile(dataSize).toPath())

        when:
        TransferManager.uploadRangesToFile(channel, fu, 1 * 1024 * 1024,
                new TransferManagerUploadToFileOptions(null,null, metadata,null)).blockingGet()
        FileGetPropertiesResponse response = fu.getProperties(null).blockingGet()

        then:
        response.statusCode() == 200
        response.headers().metadata() == metadata

        cleanup:
        channel.close()

        where:
        dataSize                                | key1  | value1 | key2   | value2
        10                                      | null  | null   | null   | null
        10                                      | "foo" | "bar"  | "fizz" | "buzz"
        FileURL.FILE_MAX_RANGE_LIMIT + 10 | null  | null   | null   | null
        FileURL.FILE_MAX_RANGE_LIMIT + 10 | "foo" | "bar"  | "fizz" | "buzz"
    }

    /*
    We require that any Flowable passed as a request body be replayable to support retries. This test ensures that
    whatever means of getting data from a file we use produces a replayable Flowable so that we abide by our own
    contract.
     */
    def "Upload replayable flowable"() {
        setup:
        // Write default data to a file
        File file = File.createTempFile(UUID.randomUUID().toString(), ".txt")
        file.deleteOnExit()
        FileOutputStream fos = new FileOutputStream(file)
        fos.write(defaultData.array())

        // Mock a response that will always be retried.
        def mockHttpResponse = Mock(HttpResponse) {
            statusCode() >> 500
            bodyAsString() >> Single.just("")
        }

        // Mock a policy that will always then check that the data is still the same and return a retryable error.
        def mockPolicy = Mock(RequestPolicy) {
            sendAsync(_) >> { HttpRequest request ->
                // Added check for request body not null since the first request while uploading ranges in to creates the file
                if (request.body()!= null && !(FlowableUtil.collectBytesInBuffer(request.body()).blockingGet() == defaultData)) {
                    throw new IllegalArgumentException()
                }
                return Single.just(mockHttpResponse)
            }
        }

        // Mock a factory that always returns our mock policy.
        def mockFactory = Mock(RequestPolicyFactory) {
            create(*_) >> mockPolicy
        }

        // Build the pipeline
        def testPipeline = HttpPipeline.build(new RequestRetryFactory(new RequestRetryOptions(null, 3, null,
                null, null)), mockFactory)
        fu = fu.withPipeline(testPipeline)
        def channel = AsynchronousFileChannel.open(file.toPath())

        when:
        TransferManager.uploadRangesToFile(channel, fu,50,null).blockingGet()

        then:
        def e = thrown(StorageException)
        e.statusCode() == 500

        cleanup:
        channel.close()
    }

    def "Upload options fail"() {
        when:
        new TransferManagerUploadToFileOptions(null,null,null,-1)

        then:
        thrown(IllegalArgumentException)
    }

    @Unroll
    def "Download file"() {
        setup:
        def channel = AsynchronousFileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE)
        TransferManager.uploadRangesToFile(channel, fu, 1 * 1024 * 1024, null)
                .blockingGet()
        def outChannel = AsynchronousFileChannel.open(getRandomFile(0).toPath(), StandardOpenOption.WRITE,
                StandardOpenOption.READ)

        when:
        TransferManager.downloadToFile(outChannel, fu,null,null).blockingGet()

        then:
        compareFiles(channel, 0, channel.size(), outChannel)

        cleanup:
        channel.close() == null
        outChannel.close() == null

        where:
        file                                   | _
        getRandomFile(20)                      | _ // small file
        getRandomFile(16 * 1024 * 1024)        | _ // medium file in several chunks
        getRandomFile(8L * 1026 * 1024 + 10)   | _ // medium file not aligned to block
        getRandomFile(0)                       | _ // empty file
    }

    def compareFiles(AsynchronousFileChannel channel1, long offset, long count, AsynchronousFileChannel channel2) {
        int chunkSize = 8 * 1024 * 1024
        long pos = 0

        while (pos < count) {
            chunkSize = Math.min(chunkSize, count - pos)
            def buf1 = FlowableUtil.collectBytesInBuffer(FlowableUtil.readFile(channel1, offset + pos, chunkSize))
                    .blockingGet()
            def buf2 = FlowableUtil.collectBytesInBuffer(FlowableUtil.readFile(channel2, pos, chunkSize)).blockingGet()

            buf1.position(0)
            buf2.position(0)

            if (buf1.compareTo(buf2) != 0) {
                return false
            }

            pos += chunkSize
        }
        if (pos != count && pos != channel2.size()) {
            return false
        }
        return true
    }

    @Unroll
    def "Download file range"() {
        setup:
        def channel = AsynchronousFileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE)
        TransferManager.uploadRangesToFile(channel, fu, 1 * 1024 * 1024, null)
                .blockingGet()
        File outFile = getRandomFile(0)
        def outChannel = AsynchronousFileChannel.open(outFile.toPath(), StandardOpenOption.WRITE,
                StandardOpenOption.READ)

        when:
        TransferManager.downloadToFile(outChannel, fu, range, null).blockingGet()

        then:
        compareFiles(channel, range.offset(), range.count(), outChannel)

        cleanup:
        channel.close()
        outChannel.close()

        where:
        file                           | range                                                        | dataSize
        getRandomFile(defaultDataSize) | new FileRange().withCount(defaultDataSize)                   | defaultDataSize
        getRandomFile(defaultDataSize) | new FileRange().withOffset(1).withCount(defaultDataSize - 1) | defaultDataSize - 1
        getRandomFile(defaultDataSize) | new FileRange().withCount(defaultDataSize - 1)               | defaultDataSize - 1
        getRandomFile(defaultDataSize) | new FileRange().withCount(10L * 1024 * 1024 * 1024)          | defaultDataSize
    }

    def "Download file count null"() {
        setup:
        fu.create(defaultDataSize, null, null, null).blockingGet()
        fu.uploadRange(new FileRange().withOffset(0).withCount(defaultDataSize), defaultFlowable, null).blockingGet()
        File outFile = getRandomFile(0)
        def outChannel = AsynchronousFileChannel.open(outFile.toPath(), StandardOpenOption.WRITE,
                StandardOpenOption.READ)

        when:
        TransferManager.downloadToFile(outChannel, fu, null, null)
                .blockingGet()

        then:
        compareDataToFile(defaultFlowable, outFile)

        cleanup:
        outChannel.close()
    }

    def "Download file etag lock"() {
        setup:
        fu.create(1 * 1024 * 1024, null, null, null).blockingGet()
        fu.uploadRange(new FileRange().withOffset(0).withCount(1 * 1024 * 1024), Flowable.just(getRandomData(1 * 1024 * 1024)), null).blockingGet()
        def outChannel = AsynchronousFileChannel.open(getRandomFile(0).toPath(), StandardOpenOption.WRITE,
                StandardOpenOption.READ)

        when:
        /*
         Set up a large download in small ranges so it makes a lot of requests. This will give us time to cut in an
         operation that will change the etag.
         */
        def success = false
        TransferManager.downloadToFile(outChannel, fu, null,
                new TransferManagerDownloadFromFileOptions(1024, null, null,
                        null, null))
                .subscribe(
                new Consumer<FileDownloadHeaders>() {
                    @Override
                    void accept(FileDownloadHeaders headers) throws Exception {
                        success = false
                    }
                },
                new Consumer<Throwable>() {
                    @Override
                    void accept(Throwable throwable) throws Exception {
                        if (throwable instanceof IllegalStateException &&
                                ((IllegalStateException) throwable).getMessage().contains("Initial eTag from the file properties doesn't matches")) {
                            success = true
                            return
                        }
                        success = false
                    }
                })


        sleep(500) // Give some time for the download request to start.
        fu.uploadRange(new FileRange().withOffset(0).withCount(defaultDataSize), defaultFlowable, null).blockingGet()

        sleep(1000) // Allow time for the upload operation

        then:
        success

        cleanup:
        outChannel.close()
    }

    @Unroll
    def "Download file options"() {
        setup:
        def channel = AsynchronousFileChannel.open(getRandomFile(defaultDataSize).toPath(), StandardOpenOption.READ,
                StandardOpenOption.WRITE)
        TransferManager.uploadRangesToFile(channel, fu, 1 * 1024 * 1024, null)
                .blockingGet()
        def outChannel = AsynchronousFileChannel.open(getRandomFile(0).toPath(), StandardOpenOption.WRITE,
                StandardOpenOption.READ)
        def reliableDownloadOptions = new ReliableDownloadOptions()
        reliableDownloadOptions.withMaxRetryRequests(retries)

        when:
        TransferManager.downloadToFile(outChannel, fu, null, new TransferManagerDownloadFromFileOptions(
                blockSize, null, null, parallelism, reliableDownloadOptions)).blockingGet()

        then:
        compareFiles(channel, 0, channel.size(), outChannel)

        cleanup:
        channel.close()
        outChannel.close()

        where:
        blockSize | parallelism | retries
        1         | null        | 2
        null      | 1           | 2
        null      | null        | 1
    }

    @Unroll
    def "Download file IA null"() {
        when:
        TransferManager.downloadToFile(file, fileURL, null, null).blockingGet()

        then:
        thrown(IllegalArgumentException)

        /*
        This test is just validating that exceptions are thrown if certain values are null. The values not being test do
        not need to be correct, simply not null. Because order in which Spock initializes values, we can't just use the
        fu property for the url.
         */
        where:
        file                                                     | fileURL
        null                                                     | new FileURL(new URL("http://account.com"), StorageURL.createPipeline(primaryCreds, new PipelineOptions()))
        AsynchronousFileChannel.open(getRandomFile(10).toPath()) | null
    }

    @Unroll
    def "Download options fail"() {
        when:
        new TransferManagerDownloadFromFileOptions(rangeLength, null, null, parallelism,
                null)

        then:
        thrown(IllegalArgumentException)

        where:
        parallelism | rangeLength
        0           | 1 * 1024
        2           | 0
    }
}

