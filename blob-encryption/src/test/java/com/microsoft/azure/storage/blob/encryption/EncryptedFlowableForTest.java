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

import com.microsoft.azure.keyvault.core.IKey;
import com.microsoft.rest.v2.util.FlowableUtil;
import io.reactivex.Flowable;
import org.reactivestreams.Subscriber;

import java.nio.ByteBuffer;
import java.security.InvalidKeyException;

/**
 * This type generates Flowables that emit ByteBuffers in specific patterns depending on test case.
 * There are nine interesting locations for the start/end of a ByteBuffer:
 * 1. Start of the blob
 * 2. Middle of offsetAdjustment
 * 3. Last byte of offsetAdjustment
 * 4. First byte of user requested data
 * 5. Middle of user requested data
 * 6. Last byte of user requested data
 * 7. First byte of end adjustment
 * 8. Middle of end adjustment
 * 9. Last byte of download
 *
 * The tests below will cover all meaningful pairs, of which there are 36, in as few distinct runs as possible.
 * The notation a/b indicates that the ByteBuffer starts at location a and ends in location b.
 */
public class EncryptedFlowableForTest extends Flowable<ByteBuffer> {

    private ByteBuffer plainText;

    private ByteBuffer cipherText;
    
    private int testCase;

    /*
    Test cases. Unfortunately because each case covers multiple combinations, the name of each case cannot provide
    much insight into to the pairs being tested, but each is commented with the pairs that it tests. There are a
    couple redundancies that are necessary to complete sending the entire data when one combination excludes using other
    new combinations.
     */

    public static final int CASE_ZERO = 0; // 1/2; 2/3; 4/5; 5/6; 7/8; 8/9

    public static final int CASE_ONE = 1; // 1/3; 4/6; 7/9

    public static final int CASE_TWO = 2; // 1/4; 5/7; 8/9

    public static final int CASE_THREE = 3; // 1/5; 6/7; 8/9

    public static final int CASE_FOUR = 4; // 1/6; 7/9

    public static final int CASE_FIVE = 5; // 1/7; 8/9

    public static final int CASE_SIX = 6; // 1/8; 8/9

    public static final int CASE_SEVEN = 7; // 1/9;

    public static final int CASE_EIGHT = 8; // 1/2; 2/4; 5/8; 8/9

    public static final int CASE_NINE = 9; // 1/2; 2/5; 6/8; 8/9

    public static final int CASE_TEN = 10; // 1/2; 2/6; 7/9

    public static final int CASE_ELEVEN  = 11; // 1/2; 2/7; 8/9

    public static final int CASE_TWELVE = 12; // 1/2; 2/8; 8/9

    public static final int CASE_THIRTEEN = 13; // 1/2; 2/9

    public static final int CASE_FOURTEEN = 14; // 1/2; 3/4; 5/9

    public static final int CASE_FIFTEEN = 15; // 1/2; 3/5; 6/9

    public static final int CASE_SIXTEEN = 16; // 1/2; 3/6; 7/9

    public static final int CASE_SEVENTEEN = 17; // 1/2; 3/7; 8/9

    public static final int CASE_EIGHTEEN = 18; // 1/2; 3/8; 8/9

    public static final int CASE_NINETEEN = 19; // 1/2; 3/9

    public static final int CASE_TWENTY = 20; // 1/3; 4/7; 8/9;

    public static final int CASE_TWENTY_ONE = 21; // 1/3; 4/8; 8/9;

    public static final int CASE_TWENTY_TWO = 22; // 1/3; 4/9;


    public EncryptedFlowableForTest(int testCase, IKey key) throws InvalidKeyException {
        this.testCase = testCase;
        switch(testCase) {
            case CASE_ZERO:
                
        }

        this.cipherText = FlowableUtil.collectBytesInBuffer(new BlobEncryptionPolicy(key, null, false)
                .encryptBlob(Flowable.just(this.plainText)).blockingGet().getCipherTextFlowable()).blockingGet();
    }

    @Override
    protected void subscribeActual(Subscriber<? super ByteBuffer> s) {
        switch(this.testCase) {
            case CASE_ZERO:

        }
    }

    public ByteBuffer getPlainText() {
        return this.plainText;
    }
}
