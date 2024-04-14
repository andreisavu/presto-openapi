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

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.connector.openapi.clientv3.model.TableMetadata;
import com.facebook.presto.spi.SchemaTableName;

import static java.util.Objects.requireNonNull;

public class OpenAPITableMetadata
{
    private final SchemaTableName schemaTableName;

    public OpenAPITableMetadata(TableMetadata metadata, TypeManager typeManager)
    {
        requireNonNull(metadata.getSchemaTableName());

        String schemaName = requireNonNull(metadata.getSchemaTableName().getSchema());
        String tableName = requireNonNull(metadata.getSchemaTableName().getTable());

        schemaTableName = new SchemaTableName(schemaName, tableName);
    }

    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }
}
