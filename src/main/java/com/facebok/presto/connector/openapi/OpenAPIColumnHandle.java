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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;

import static java.util.Objects.requireNonNull;

public class OpenAPIColumnHandle
        implements ColumnHandle
{
    private final ColumnMetadata columnMetadata;

    public OpenAPIColumnHandle(ColumnMetadata columnMetadata)
    {
        this.columnMetadata = requireNonNull(columnMetadata);
    }

    public ColumnMetadata getColumnMetadata()
    {
        return columnMetadata;
    }
}
