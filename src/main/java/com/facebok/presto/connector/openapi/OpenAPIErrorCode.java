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

import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.common.ErrorType;
import com.facebook.presto.spi.ErrorCodeSupplier;

public enum OpenAPIErrorCode
        implements ErrorCodeSupplier
{
    OPENAPI_NOT_IMPLEMENTED(1, ErrorType.EXTERNAL),
    OPENAPI_INVALID_RESPONSE(2, ErrorType.EXTERNAL),
    OPENAPI_GENERIC_SERVICE_ERROR(3, ErrorType.EXTERNAL);

    private final ErrorCode errorCode;

    OpenAPIErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0105, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
