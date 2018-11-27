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
package com.microsoft.azure.storage.file;

import java.net.URL;
import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * A class used to conveniently parse URLs into {@link FileURLParts} to modify the components of the URL.
 */
public final class URLParser {

    // ipv4PatternString represents the pattern of ipv4 addresses.
    private final static String ipv4PatternString = "^((0|1\\d?\\d?|2[0-4]?\\d?|25[0-5]?|[3-9]\\d?)\\.){3}(0|1\\d?\\d?|2[0-4]?\\d?|25[0-5]?|[3-9]\\d?)$";
    // ipv4Pattern represents a compiled pattern of ipv4 address pattern.
    private static Pattern ipv4Pattern = Pattern.compile(ipv4PatternString);

    /**
     * URLParser parses a URL initializing FileURLParts' fields including any SAS-related query parameters.
     * Any other query parameters remain in the UnparsedParams field. This method overwrites all fields in the
     * QueueUrlParts object.
     *
     * @param url
     *         The {@code URL} to be parsed.
     *
     * @return A {@link FileURLParts} object containing all the components of a FileURL.
     *
     * @throws UnknownHostException
     *         If the url contains an improperly formatted ip address or unknown host address.
     */
    public static FileURLParts parse(URL url) throws UnknownHostException {

        final String scheme = url.getProtocol();
        final String host = url.getHost();

        String shareName = null;
        String directoryOrFilePath = null;
        IPEndPointStyleInfo ipEndPointStyleInfo = null;

        String path = url.getPath();
        if (!Utility.isNullOrEmpty(path)) {
            // if the path starts with a slash remove it
            if (path.charAt(0) == '/') {
                path = path.substring(1);
            }

            // If the host name is in the ip-address format
            if (isHostIPEndPointStyle(host)) {
                // Create the IPEndPointStyleInfo and set the port number.
                ipEndPointStyleInfo = new IPEndPointStyleInfo();
                if (url.getPort() != -1) {
                    ipEndPointStyleInfo.withPort(url.getPort());
                }
                String accountName;
                /*
                  If the host is in the IP Format, then account name is provided after the ip-address.
                  For Example: "https://10.132.141.33/accountname/shareName". Get the index of "/" in the path.
                 */
                int pathSepEndIndex = path.indexOf('/');
                if (pathSepEndIndex == -1) {
                    /*
                     If there does not exists "/" in the path, it means the entire path is the account name
                     For Example: path = accountname
                     */
                    accountName = path;
                    // since path contains only the account name, after account name is set, set path to empty string.
                    path = Constants.EMPTY_STRING;
                } else {
                    /*
                     If there exists the "/", it means all the content in the path till "/" is the account name
                     For Example: accountname/shareName/dir1/file1
                     */
                    accountName = path.substring(0, pathSepEndIndex);
                    // After accoutname is from the path, change the path.
                    path = path.substring(pathSepEndIndex + 1);
                }
                ipEndPointStyleInfo.withAccountName(accountName);
            }

            int pathSepEndIndex = path.indexOf('/');
            if (pathSepEndIndex == -1) {
                // path contains only a shareName and no directory or file path.
                shareName = path;
            } else {
                // path contains the share name up until the "/" and directory or file path is everything after the slash.
                shareName = path.substring(0, pathSepEndIndex);

                directoryOrFilePath = path.substring(pathSepEndIndex + 1);
            }
        }
        Map<String, String[]> queryParamsMap = parseQueryString(url.getQuery());

        String shareSnapshot = null;
        String[] snapshotArray = queryParamsMap.get(Constants.SNAPSHOT_QUERY_PARAMETER);
        if (snapshotArray != null) {
            shareSnapshot = snapshotArray[0];
            queryParamsMap.remove(Constants.SNAPSHOT_QUERY_PARAMETER);
        }

        SASQueryParameters sasQueryParameters = new SASQueryParameters(queryParamsMap, true);

        return new FileURLParts()
                .withScheme(scheme)
                .withHost(host)
                .withShareName(shareName)
                .withDirectoryOrFilePath(directoryOrFilePath)
                .withShareSnapshot(shareSnapshot)
                .withSasQueryParameters(sasQueryParameters)
                .withUnparsedParameters(queryParamsMap)
                .withIPEndPointStyleInfo(ipEndPointStyleInfo);
    }

    /**
     * Parses a query string into a one to many hashmap.
     *
     * @param queryParams
     *         The string of query params to parse.
     *
     * @return A {@code HashMap<String, String[]>} of the key values.
     */
    private static TreeMap<String, String[]> parseQueryString(String queryParams) {

        final TreeMap<String, String[]> retVals = new TreeMap<String, String[]>(new Comparator<String>() {
            @Override
            public int compare(String s1, String s2) {
                return s1.compareTo(s2);
            }
        });

        if (Utility.isNullOrEmpty(queryParams)) {
            return retVals;
        }

        // split name value pairs by splitting on the 'c&' character
        final String[] valuePairs = queryParams.split("&");

        // for each field value pair parse into appropriate map entries
        for (int m = 0; m < valuePairs.length; m++) {
            // Getting key and value for a single query parameter
            final int equalDex = valuePairs[m].indexOf("=");
            String key = Utility.safeURLDecode(valuePairs[m].substring(0, equalDex)).toLowerCase(Locale.ROOT);
            String value = Utility.safeURLDecode(valuePairs[m].substring(equalDex + 1));

            // add to map
            String[] keyValues = retVals.get(key);

            // check if map already contains key
            if (keyValues == null) {
                // map does not contain this key
                keyValues = new String[]{value};
                retVals.put(key, keyValues);
            } else {
                // map contains this key already so append
                final String[] newValues = new String[keyValues.length + 1];
                for (int j = 0; j < keyValues.length; j++) {
                    newValues[j] = keyValues[j];
                }

                newValues[newValues.length - 1] = value;
            }
        }

        return retVals;
    }

    /**
     * Checks if URL's host is IP. If true storage account endpoint will be composed as: http(s)://IP(:port)/storageaccount/share(||container||etc)/...
     * As url's Host property, host could be both host or host:port
     *
     * @param host
     *         The host. Ex: "account.queue.core.windows.net" or 10.31.141.33:80
     *
     * @return true, if the host is an ip address (ipv4/ipv6) with or without port.
     */
    public static boolean isHostIPEndPointStyle(String host) {
        if ((host == null) || host.equals("")) {
            return false;
        }
        // returns true if host represents an ipv4 address.
        return ipv4Pattern.matcher(host).matches();
    }
}
