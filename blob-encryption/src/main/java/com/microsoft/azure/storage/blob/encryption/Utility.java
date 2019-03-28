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

package com.microsoft.azure.storage.blob.encryption;

import java.util.Locale;

/**
 * RESERVED FOR INTERNAL USE. A class which provides utility methods.
 */
final class Utility {
    /**
     * Asserts that a value is not <code>null</code>.
     *
     * @param param
     *         A {@code String} that represents the name of the parameter, which becomes the exception message
     *         text if the <code>value</code> parameter is <code>null</code>.
     * @param value
     *         An <code>Object</code> object that represents the value of the specified parameter. This is the value
     *         being asserted as not <code>null</code>.
     */
    static void assertNotNull(final String param, final Object value) {
        if (value == null) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "The argument must not be null or an empty string. Argument name: %s.", param));
        }
    }
}
