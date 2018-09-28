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

package com.microsoft.azure.storage.blob;

import com.microsoft.rest.v2.Context;
import com.microsoft.rest.v2.http.HttpRequest;
import com.microsoft.rest.v2.http.HttpResponse;
import com.microsoft.rest.v2.policy.RequestPolicy;
import com.microsoft.rest.v2.policy.RequestPolicyFactory;
import com.microsoft.rest.v2.policy.RequestPolicyOptions;
import io.reactivex.Single;

public final class ProgressReportingFactory implements RequestPolicyFactory{

    static final String progressContextKey = "ParallelProgressTracker";

    @Override
    public RequestPolicy create(RequestPolicy nextPolicy, RequestPolicyOptions requestPolicyOptions) {
        return new ProgressReportingPolicy(nextPolicy);
    }

    private final class ProgressReportingPolicy implements RequestPolicy {

        private final RequestPolicy nextPolicy;

        private ProgressReportingPolicy(RequestPolicy nextPolicy){
            this.nextPolicy = nextPolicy;
        }

        @Override
        public Single<HttpResponse> sendAsync(HttpRequest httpRequest) {
            return nextPolicy.sendAsync(httpRequest)
                    // Process exceptions.
                    .doOnError(throwable -> {
                        String action = RequestRetryFactory.processExceptionForRetryability(throwable);
                        this.tryRewindProgress(action, httpRequest.context());
                    })
                    // Process the status code in case the request was successful but with a failure error code.
                    .doOnSuccess(httpResponse -> {
                        /*
                        Always specifying that we are not trying trying primary means we will always rewind in the case
                        of 404, even if we actually are trying the primary. This is ok because in this case the request
                        is going to fail with an exception anyway, so any progress is irrelevant and the behavior
                        will be correct in the event we are trying the secondary.
                         */
                        String action = RequestRetryFactory.processStatusCodeForRetryability(httpResponse.statusCode(),
                                false);
                        this.tryRewindProgress(action, httpRequest.context());
                    });
        }

        private void tryRewindProgress(String action, Context context) {
            if (action.charAt(0) == 'R') {
                try {
                    context.getData(progressContextKey)
                            .ifPresent(object -> {
                                ParallelProgressTracker tracker = (ParallelProgressTracker)object;
                                tracker.rewindProgress();
                            });
                }
                /*
                There is a bug in the autorest Context class that will throw when trying to get the data from a
                Context.NONE. This is a temporary workaround.
                 */
                catch (NullPointerException e) {
                }
            }
        }
    }
}
