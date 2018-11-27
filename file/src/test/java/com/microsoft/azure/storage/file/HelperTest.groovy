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

package com.microsoft.azure.storage.file

import com.microsoft.azure.storage.APISpec
import com.microsoft.azure.storage.file.models.AccessPolicy
import com.microsoft.azure.storage.file.models.ShareSetAccessPolicyResponse
import com.microsoft.azure.storage.file.models.SignedIdentifier
import com.microsoft.azure.storage.file.models.StorageErrorCode
import spock.lang.Unroll

import java.time.OffsetDateTime
import java.time.ZoneOffset

class HelperTest extends APISpec {

    def "responseError"() {
        when:
        su.listSharesSegment("garbage", null, null).blockingGet()

        then:
        def e = thrown(StorageException)
        e.errorCode() == StorageErrorCode.OUT_OF_RANGE_INPUT
        e.statusCode() == 400
        e.message().contains("One of the request inputs is out of range.")
        e.getMessage().contains("<?xml") // Ensure that the details in the payload are printable
    }

    @Unroll
    def "File range"() {
        expect:
        new FileRange().withOffset(offset).withCount(count).toString() == result

        where:
        offset | count || result
        0      | null  || "bytes=0-"
        0      | 5     || "bytes=0-4"
    }

    @Unroll
    def "File range IA"() {
        when:
        new FileRange().withOffset(offset).withCount(count)

        then:
        thrown(IllegalArgumentException)

        where:
        offset | count
        -1     | 5
        0      | -1
    }

    def "serviceSASSignatureValues network test share"() {
        setup:
        def shareName = generateShareName()
        def shu = primaryServiceURL.createShareURL(shareName)
        shu.create(null, null, null).blockingGet()
        def id = new SignedIdentifier().withId("0000").withAccessPolicy(new AccessPolicy().withPermission("rcwdl")
                .withExpiry(OffsetDateTime.now().plusDays(2)))
        ShareSetAccessPolicyResponse rep = shu.setAccessPolicy(Arrays.asList(id), null).blockingGet()

        // Check id field
        def v = new ServiceSASSignatureValues()
                .withIdentifier(id.id())
                .withShareName(shareName)
                .withProtocol(SASProtocol.HTTPS_ONLY)

        // Check another serviceSASSignature instance without identifier.
        def v2 = new ServiceSASSignatureValues()

        // Create new shareSASPermissions
        def p = new ShareSASPermission()
                .withRead(true)
                .withWrite(true)
                .withCreate(true)
                .withDelete(true)
                .withList(true)

        v2.withPermissions(p.toString())
                .withExpiryTime(OffsetDateTime.now().plusDays(1))
                .withShareName(shareName)

        when:
        // Create a directoryURL from shareURL with ServiceSASSignatureValues v1(with identifier)
        def parts = URLParser.parse(shu.createDirectoryURL("dir1").toURL())
                .withSasQueryParameters(v.generateSASQueryParameters(primaryCreds))
                .withScheme("https")
        def duSAS = new DirectoryURL(parts.toURL(), StorageURL.createPipeline(new AnonymousCredentials(),
                new PipelineOptions()))
        // Set the SasQueryParameters in parts to another ServiceSASSignatureValues instance v2 (without identifier)
        parts.withSasQueryParameters(v2.generateSASQueryParameters(primaryCreds))
        parts.withDirectoryOrFilePath("dir2")
        def duSAS1 = new DirectoryURL(parts.toURL(), StorageURL.createPipeline(new AnonymousCredentials(),
                new PipelineOptions()))

        then:
        duSAS.create(null, null).blockingGet()
        duSAS1.create(null, null).blockingGet()
        notThrown(StorageException)
    }

    def "serviceSASSignatureValues network test directory"() {
        setup:
        def shareName = generateShareName()
        def directoryPath = generateDirectoryName()
        def shu = su.createShareURL(shareName)
        shu.create(null, null, null).blockingGet()

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

        def ipR = new IPRange()
                .withIpMin("0.0.0.0")
                .withIpMax("255.255.255.255")
        v.withIpRange(ipR)
                .withProtocol(SASProtocol.HTTPS_ONLY)
                .withCacheControl("cache")
                .withContentDisposition("disposition")
                .withContentEncoding("encoding")
                .withContentLanguage("language")
                .withContentType("type")

        when:
        def parts = URLParser.parse(shu.createDirectoryURL(directoryPath).toURL())
        parts.withSasQueryParameters(v.generateSASQueryParameters(primaryCreds)).withScheme("https")
        parts.withDirectoryOrFilePath(directoryPath)
        def du = new DirectoryURL(parts.toURL(), StorageURL.createPipeline(new AnonymousCredentials(),
                new PipelineOptions()))

        then:
        du.create(null, null).blockingGet()

        and:
        def properties = du.getProperties(null).blockingGet().headers()

        then:
        validateResponseHeaders(properties)
    }
    /*
     This test will ensure that each field gets placed into the proper location within the string to sign and that null
     values are handled correctly. We will validate the whole SAS with service calls as well as correct serialization of
     individual parts later.
     */

    @Unroll
    def "serviceSasSignatures string to sign"() {
        when:
        def v = new ServiceSASSignatureValues()
        if (permissions != null) {
            def p = new FileSASPermission()
            p.withRead(true)
            v.withPermissions(p.toString())
        }
        v.withStartTime(startTime)
                .withExpiryTime(expiryTime)
                .withShareName("s")
        if (ipRange != null) {
            def ipR = new IPRange()
            ipR.withIpMin("ip")
            v.withIpRange(ipR)
        }
        v.withIdentifier(identifier)
                .withProtocol(protocol)
                .withCacheControl(cacheControl)
                .withContentDisposition(disposition)
                .withContentEncoding(encoding)
                .withContentLanguage(language)
                .withContentType(type)

        def token = v.generateSASQueryParameters(primaryCreds)

        then:
        token.signature() == primaryCreds.computeHmac256(expectedStringToSign)

        /*
        We don't test the file or shareName properties because canonicalized resource is always added as at least
        /file/accountName. We test canonicalization of resources later. Again, this is not to test a fully functional
        sas but the construction of the string to sign.
         */
        where:
        permissions             | startTime                                                 | expiryTime                                                | identifier | ipRange       | protocol               | cacheControl | disposition   | encoding   | language   | type   || expectedStringToSign
        new FileSASPermission() | null                                                      | null                                                      | null       | null          | null                   | null         | null          | null       | null       | null   || "r\n\n\n" + "/file/" + primaryCreds.getAccountName() + "/s\n\n\n\n" + Constants.HeaderConstants.TARGET_STORAGE_VERSION + "\n\n\n\n\n"
        null                    | OffsetDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC) | null                                                      | null       | null          | null                   | null         | null          | null       | null       | null   || "\n" + Utility.ISO8601UTCDateFormatter.format(OffsetDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)) + "\n\n/file/" + primaryCreds.getAccountName() + "/s\n\n\n\n" + Constants.HeaderConstants.TARGET_STORAGE_VERSION + "\n\n\n\n\n"
        null                    | null                                                      | OffsetDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC) | null       | null          | null                   | null         | null          | null       | null       | null   || "\n\n" + Utility.ISO8601UTCDateFormatter.format(OffsetDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)) + "\n/file/" + primaryCreds.getAccountName() + "/s\n\n\n\n" + Constants.HeaderConstants.TARGET_STORAGE_VERSION + "\n\n\n\n\n"
        null                    | null                                                      | null                                                      | "id"       | null          | null                   | null         | null          | null       | null       | null   || "\n\n\n/file/" + primaryCreds.getAccountName() + "/s\nid\n\n\n" + Constants.HeaderConstants.TARGET_STORAGE_VERSION + "\n\n\n\n\n"
        null                    | null                                                      | null                                                      | null       | new IPRange() | null                   | null         | null          | null       | null       | null   || "\n\n\n/file/" + primaryCreds.getAccountName() + "/s\n\nip\n\n" + Constants.HeaderConstants.TARGET_STORAGE_VERSION + "\n\n\n\n\n"
        null                    | null                                                      | null                                                      | null       | null          | SASProtocol.HTTPS_ONLY | null         | null          | null       | null       | null   || "\n\n\n/file/" + primaryCreds.getAccountName() + "/s\n\n\n" + SASProtocol.HTTPS_ONLY + "\n" + Constants.HeaderConstants.TARGET_STORAGE_VERSION + "\n\n\n\n\n"
        null                    | null                                                      | null                                                      | null       | null          | null                   | "control"    | null          | null       | null       | null   || "\n\n\n/file/" + primaryCreds.getAccountName() + "/s\n\n\n\n" + Constants.HeaderConstants.TARGET_STORAGE_VERSION + "\ncontrol\n\n\n\n"
        null                    | null                                                      | null                                                      | null       | null          | null                   | null         | "disposition" | null       | null       | null   || "\n\n\n/file/" + primaryCreds.getAccountName() + "/s\n\n\n\n" + Constants.HeaderConstants.TARGET_STORAGE_VERSION + "\n\ndisposition\n\n\n"
        null                    | null                                                      | null                                                      | null       | null          | null                   | null         | null          | "encoding" | null       | null   || "\n\n\n/file/" + primaryCreds.getAccountName() + "/s\n\n\n\n" + Constants.HeaderConstants.TARGET_STORAGE_VERSION + "\n\n\nencoding\n\n"
        null                    | null                                                      | null                                                      | null       | null          | null                   | null         | null          | null       | "language" | null   || "\n\n\n/file/" + primaryCreds.getAccountName() + "/s\n\n\n\n" + Constants.HeaderConstants.TARGET_STORAGE_VERSION + "\n\n\n\nlanguage\n"
        null                    | null                                                      | null                                                      | null       | null          | null                   | null         | null          | null       | null       | "type" || "\n\n\n/file/" + primaryCreds.getAccountName() + "/s\n\n\n\n" + Constants.HeaderConstants.TARGET_STORAGE_VERSION + "\n\n\n\n\ntype"
    }

    @Unroll
    def "serviceSASSignatureValues canonicalizedResource"() {
        setup:
        def v = new ServiceSASSignatureValues()
                .withShareName(shareName)
                .withFilePath(filePath)

        when:
        def token = v.generateSASQueryParameters(primaryCreds)

        then:
        token.signature() == primaryCreds.computeHmac256(expectedStringToSign)
        token.resource() == expectedResource

        where:
        shareName | filePath || expectedResource | expectedStringToSign
        "s"       | "f"      || "f"              | "\n\n\n" + "/file/" + primaryCreds.getAccountName() + "/s/f\n\n\n\n" + Constants.HeaderConstants.TARGET_STORAGE_VERSION + "\n\n\n\n\n"
        "s"       | null     || "s"              | "\n\n\n" + "/file/" + primaryCreds.getAccountName() + "/s\n\n\n\n" + Constants.HeaderConstants.TARGET_STORAGE_VERSION + "\n\n\n\n\n"

    }

    @Unroll
    def "serviceSasSignatureValues IA"() {
        setup:
        def v = new ServiceSASSignatureValues()
                .withShareName(shareName)
                .withVersion(version)

        when:
        v.generateSASQueryParameters(creds)

        then:
        def e = thrown(IllegalArgumentException)
        e.getMessage().contains(parameter)

        where:
        shareName | version | creds       || parameter
        null      | "v"     | primaryCreds | "share"
        "s"       | null    | primaryCreds | "version"
        "s"       | "v"     | null         | "sharedKeyCredentials"
    }

    @Unroll
    def "FileSASPermission toString"() {
        setup:
        def perms = new FileSASPermission()
                .withRead(read)
                .withWrite(write)
                .withDelete(delete)
                .withCreate(create)

        expect:
        perms.toString() == expectedString

        where:
        read  | write | delete | create || expectedString
        true  | false | false  | false  || "r"
        false | true  | false  | false  || "w"
        false | false | true   | false  || "d"
        false | false | false  | true   || "c"
        true  | true  | true   | true   || "rcwd"
    }

    @Unroll
    def "FileSASPermission parse"() {
        when:
        def perms = FileSASPermission.parse(permString)

        then:
        perms.read() == read
        perms.write() == write
        perms.delete() == delete
        perms.create() == create

        where:
        permString || read  | write | delete | create
        "r"        || true  | false | false  | false
        "w"        || false | true  | false  | false
        "d"        || false | false | true   | false
        "c"        || false | false | false  | true
        "rcwd"     || true  | true  | true   | true
        "dcwr"     || true  | true  | true   | true
    }

    def "FileSASPermission parse IA"() {
        when:
        FileSASPermission.parse("rwcad")

        then:
        thrown(IllegalArgumentException)
    }

    @Unroll
    def "ShareSASPermission toString"() {
        setup:
        def perms = new ShareSASPermission()
                .withRead(read)
                .withWrite(write)
                .withDelete(delete)
                .withCreate(create)
                .withAdd(add)
                .withList(list)

        expect:
        perms.toString() == expectedString

        where:
        read  | write | delete | create | add   | list  || expectedString
        true  | false | false  | false  | false | false || "r"
        false | true  | false  | false  | false | false || "w"
        false | false | true   | false  | false | false || "d"
        false | false | false  | true   | false | false || "c"
        false | false | false  | false  | true  | false || "a"
        false | false | false  | false  | false | true  || "l"
        true  | true  | true   | true   | true  | true  || "racwdl"
    }

    @Unroll
    def "ShareSASPermission parse"() {
        when:
        def perms = ShareSASPermission.parse(permString)

        then:
        perms.read() == read
        perms.write() == write
        perms.delete() == delete
        perms.create() == create
        perms.add() == add
        perms.list() == list

        where:
        permString || read  | write | delete | create | add   | list
        "r"        || true  | false | false  | false  | false | false
        "w"        || false | true  | false  | false  | false | false
        "d"        || false | false | true   | false  | false | false
        "c"        || false | false | false  | true   | false | false
        "a"        || false | false | false  | false  | true  | false
        "l"        || false | false | false  | false  | false | true
        "racwdl"   || true  | true  | true   | true   | true  | true
        "dcwrla"   || true  | true  | true   | true   | true  | true
    }

    def "ShareSASPermission parse IA"() {
        when:
        ShareSASPermission.parse("rwaq")

        then:
        thrown(IllegalArgumentException)
    }

    @Unroll
    def "IPRange toString"() {
        setup:
        def ip = new IPRange()
                .withIpMin(min)
                .withIpMax(max)

        expect:
        ip.toString() == expectedString

        where:
        min  | max  || expectedString
        "a"  | "b"  || "a-b"
        "a"  | null || "a"
        null | "b"  || ""
    }

    @Unroll
    def "IPRange parse"() {
        when:
        def ip = IPRange.parse(rangeStr)

        then:
        ip.ipMin() == min
        ip.ipMax() == max

        where:
        rangeStr || min | max
        "a-b"    || "a" | "b"
        "a"      || "a" | null
        ""       || ""  | null
    }

    @Unroll
    def "SASProtocol parse"() {
        expect:
        SASProtocol.parse(protocolStr) == protocol

        where:
        protocolStr  || protocol
        "https"      || SASProtocol.HTTPS_ONLY
        "https,http" || SASProtocol.HTTPS_HTTP
    }

    /*
     This test will ensure that each field gets placed into the proper location within the string to sign and that null
     values are handled correctly. We will validate the whole SAS with service calls as well as correct serialization of
     individual parts later.
     */

    @Unroll
    def "accountSasSignatures string to sign"() {
        when:
        def v = new AccountSASSignatureValues()
        def p = new AccountSASPermission()
                .withRead(true)
        v.withPermissions(p.toString())
                .withServices("b")
                .withResourceTypes("o")
                .withStartTime(startTime)
                .withExpiryTime(OffsetDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC))
        if (ipRange != null) {
            def ipR = new IPRange()
            ipR.withIpMin("ip")
            v.withIpRange(ipR)
        }
        v.withProtocol(protocol)

        def token = v.generateSASQueryParameters(primaryCreds)

        then:
        token.signature() == primaryCreds.computeHmac256(expectedStringToSign)

        where:
        startTime                                                 | ipRange       | protocol               || expectedStringToSign
        OffsetDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC) | null          | null                   || primaryCreds.getAccountName() + "\nr\nb\no\n" + Utility.ISO8601UTCDateFormatter.format(OffsetDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)) + "\n" + Utility.ISO8601UTCDateFormatter.format(OffsetDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)) + "\n\n\n" + Constants.HeaderConstants.TARGET_STORAGE_VERSION + "\n"
        null                                                      | new IPRange() | null                   || primaryCreds.getAccountName() + "\nr\nb\no\n\n" + Utility.ISO8601UTCDateFormatter.format(OffsetDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)) + "\nip\n\n" + Constants.HeaderConstants.TARGET_STORAGE_VERSION + "\n"
        null                                                      | null          | SASProtocol.HTTPS_ONLY || primaryCreds.getAccountName() + "\nr\nb\no\n\n" + Utility.ISO8601UTCDateFormatter.format(OffsetDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)) + "\n\n" + SASProtocol.HTTPS_ONLY + "\n" + Constants.HeaderConstants.TARGET_STORAGE_VERSION + "\n"
    }

    @Unroll
    def "accountSasSignatureValues IA"() {
        setup:
        def v = new AccountSASSignatureValues()
                .withPermissions(permissions)
                .withServices(service)
                .withResourceTypes(resourceType)
                .withExpiryTime(expiryTime)
                .withVersion(version)

        when:
        v.generateSASQueryParameters(creds)

        then:
        def e = thrown(IllegalArgumentException)
        e.getMessage().contains(parameter)

        where:
        permissions | service | resourceType | expiryTime           | version | creds        || parameter
        null        | "b"     | "c"          | OffsetDateTime.now() | "v"     | primaryCreds || "permissions"
        "c"         | null    | "c"          | OffsetDateTime.now() | "v"     | primaryCreds || "services"
        "c"         | "b"     | null         | OffsetDateTime.now() | "v"     | primaryCreds || "resourceTypes"
        "c"         | "b"     | "c"          | null                 | "v"     | primaryCreds || "expiryTime"
        "c"         | "b"     | "c"          | OffsetDateTime.now() | null    | primaryCreds || "version"
        "c"         | "b"     | "c"          | OffsetDateTime.now() | "v"     | null         || "SharedKeyCredentials"
    }

    @Unroll
    def "AccountSASPermissions toString"() {
        setup:
        def perms = new AccountSASPermission()
        perms.withRead(read)
                .withWrite(write)
                .withDelete(delete)
                .withList(list)
                .withAdd(add)
                .withCreate(create)
                .withUpdate(update)
                .withProcessMessages(process)

        expect:
        perms.toString() == expectedString

        where:
        read  | write | delete | list  | add   | create | update | process || expectedString
        true  | false | false  | false | false | false  | false  | false   || "r"
        false | true  | false  | false | false | false  | false  | false   || "w"
        false | false | true   | false | false | false  | false  | false   || "d"
        false | false | false  | true  | false | false  | false  | false   || "l"
        false | false | false  | false | true  | false  | false  | false   || "a"
        false | false | false  | false | false | true   | false  | false   || "c"
        false | false | false  | false | false | false  | true   | false   || "u"
        false | false | false  | false | false | false  | false  | true    || "p"
        true  | true  | true   | true  | true  | true   | true   | true    || "rwdlacup"
    }

    @Unroll
    def "AccountSASPermissions parse"() {
        when:
        def perms = AccountSASPermission.parse(permString)

        then:
        perms.read() == read
        perms.write() == write
        perms.delete() == delete
        perms.list() == list
        perms.add() == add
        perms.create() == create
        perms.update() == update
        perms.processMessages() == process

        where:
        permString || read  | write | delete | list  | add   | create | update | process
        "r"        || true  | false | false  | false | false | false  | false  | false
        "w"        || false | true  | false  | false | false | false  | false  | false
        "d"        || false | false | true   | false | false | false  | false  | false
        "l"        || false | false | false  | true  | false | false  | false  | false
        "a"        || false | false | false  | false | true  | false  | false  | false
        "c"        || false | false | false  | false | false | true   | false  | false
        "u"        || false | false | false  | false | false | false  | true   | false
        "p"        || false | false | false  | false | false | false  | false  | true
        "rwdlacup" || true  | true  | true   | true  | true  | true   | true   | true
        "lwrupcad" || true  | true  | true   | true  | true  | true   | true   | true
    }

    def "AccountSASPermissions parse IA"() {
        when:
        AccountSASPermission.parse("rwaq")

        then:
        thrown(IllegalArgumentException)
    }

    @Unroll
    def "AccountSASResourceType toString"() {
        setup:
        def resourceTypes = new AccountSASResourceType()
                .withService(service)
                .withContainer(container)
                .withObject(object)

        expect:
        resourceTypes.toString() == expectedString

        where:
        service | container | object || expectedString
        true    | false     | false  || "s"
        false   | true      | false  || "c"
        false   | false     | true   || "o"
        true    | true      | true   || "sco"
    }

    @Unroll
    def "AccountSASResourceType parse"() {
        when:
        def resourceTypes = AccountSASResourceType.parse(resourceTypeString)

        then:
        resourceTypes.service() == service
        resourceTypes.container() == container
        resourceTypes.object() == object

        where:
        resourceTypeString || service | container | object
        "s"                || true    | false     | false
        "c"                || false   | true      | false
        "o"                || false   | false     | true
        "sco"              || true    | true      | true
    }

    @Unroll
    def "AccountSASResourceType IA"() {
        when:
        AccountSASResourceType.parse("scq")

        then:
        thrown(IllegalArgumentException)
    }

    def "FileURLParts"() {
        setup:
        def parts = new FileURLParts()
        parts.withScheme("http")
                .withHost("host")
                .withShareName("shareName")
                .withDirectoryOrFilePath("filePath")
                .withShareSnapshot("shareSnapshot")
        def sasValues = new ServiceSASSignatureValues()
                .withPermissions("r")
                .withShareName("shareName")
        parts.withSasQueryParameters(sasValues.generateSASQueryParameters(primaryCreds))

        when:
        def splitParts = parts.toURL().toString().split("\\?")

        then:
        splitParts.size() == 2 // Ensure that there is only one question mark even when sas and snapshot are present
        splitParts[0] == "http://host/shareName/filePath"
        splitParts[1].contains("snapshot=shareSnapshot")
        splitParts[1].contains("sp=r")
        splitParts[1].contains("sig=")
        splitParts[1].split("&").size() == 5 // snapshot & sv & sr & sp & sig
    }

    def "URLParser"() {
        when:
        def parts = URLParser.parse(new URL("http://host/share/file?sharesnapshot=snapshot&sv=2018-03-28&sr=c&sp=r&sig=Ee%2BSodSXamKSzivSdRTqYGh7AeMVEk3wEoRZ1yzkpSc%3D"))

        then:
        parts.scheme() == "http"
        parts.host() == "host"
        parts.shareName() == "share"
        parts.directoryOrFilePath() == "file"
        parts.shareSnapshot() == "snapshot"
        parts.sasQueryParameters().permissions() == "r"
        parts.sasQueryParameters().version() == Constants.HeaderConstants.TARGET_STORAGE_VERSION
        parts.sasQueryParameters().resource() == "c"
        parts.sasQueryParameters().signature() ==
                Utility.safeURLDecode("Ee%2BSodSXamKSzivSdRTqYGh7AeMVEk3wEoRZ1yzkpSc%3D")
    }
}
