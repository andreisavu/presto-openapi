/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebok.presto.connector.openapi;

import com.facebook.presto.connector.openapi.clientv3.ApiException;
import com.facebook.presto.connector.openapi.clientv3.JSON;
import com.facebook.presto.connector.openapi.clientv3.model.Error;

public class OpenAPIServiceException
        extends RuntimeException
{
    private final Error error;
    private final int statusCode;

    public OpenAPIServiceException(ApiException e)
    {
        super(e.getMessage(), e);

        Error parsedError;
        this.statusCode = e.getCode();

        // Make a best effort attempt to deserialize the error message from the response body
        if (e.getResponseBody() == null) {
            parsedError = new Error().message(e.getMessage()).retryable(false);
        }
        else {
            try {
                parsedError = JSON.deserialize(e.getResponseBody(), Error.class);
            }
            catch (Exception ex) {
                parsedError = new Error().message(e.getMessage()).retryable(false);
            }
        }
        this.error = parsedError;
    }

    public Error getError()
    {
        return error;
    }

    public int getStatusCode()
    {
        return statusCode;
    }

    public boolean isRetryable()
    {
        return error.getRetryable();
    }

    @Override
    public String getMessage()
    {
        return error.getMessage();
    }
}
