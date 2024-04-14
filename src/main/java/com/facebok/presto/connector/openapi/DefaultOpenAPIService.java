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

import com.facebook.presto.connector.openapi.clientv3.model.SchemaTable;
import com.facebook.presto.connector.openapi.clientv3.model.TableMetadata;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import javax.annotation.Nullable;

import java.util.List;

public class DefaultOpenAPIService
        implements OpenAPIService
{
    private final String baseUrl;

    @Inject
    DefaultOpenAPIService(OpenAPIConnectorConfig config)
    {
        this.baseUrl = config.getBaseUrl();
    }

    @Override
    public List<String> listSchemaNames()
    {
        return ImmutableList.of("schema1");
    }

    @Override
    public List<SchemaTable> listTables(@Nullable String schemaOrNull)
    {
        return ImmutableList.of(
                new SchemaTable().schema("schema1").table("table1"));
    }

    @Override
    public TableMetadata getTableMetadata(SchemaTable schemaTable)
    {
        return null;
    }
}
