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

import com.facebook.presto.connector.openapi.clientv3.ApiClient;
import com.facebook.presto.connector.openapi.clientv3.Configuration;
import com.facebook.presto.connector.openapi.clientv3.api.DefaultApi;
import com.facebook.presto.connector.openapi.clientv3.model.SchemaTable;
import com.facebook.presto.connector.openapi.clientv3.model.SchemasSchemaTablesTableSplitsPostRequest;
import com.facebook.presto.connector.openapi.clientv3.model.Splits;
import com.facebook.presto.connector.openapi.clientv3.model.TableMetadata;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class DefaultOpenAPIService
        implements OpenAPIService
{
    private final DefaultApi defaultApi;
    private final URI baseURI;

    @Inject
    DefaultOpenAPIService(OpenAPIConnectorConfig config)
    {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath(config.getBaseUrl());
        this.baseURI = URI.create(config.getBaseUrl());
        this.defaultApi = new DefaultApi(defaultClient);
    }

    @Override
    public URI getBaseURI()
    {
        return baseURI;
    }

    @Override
    public List<String> listSchemaNames()
    {
        List<String> schemas = defaultApi.schemasGet();
        return ImmutableList.copyOf(schemas);
    }

    @Override
    public List<SchemaTable> listTables(@Nullable String schemaOrNull)
    {
        List<String> schemas = new ArrayList<>();
        if (schemaOrNull == null) {
            schemas.addAll(listSchemaNames());
        }
        else {
            schemas.add(schemaOrNull);
        }
        List<SchemaTable> result = new ArrayList<>();
        for (String schemaName : schemas) {
            result.addAll(defaultApi.schemasSchemaTablesGet(schemaName));
        }
        return ImmutableList.copyOf(result);
    }

    @Override
    public TableMetadata getTableMetadata(SchemaTable schemaTable)
    {
        return defaultApi.schemasSchemaTablesTableGet(schemaTable.getSchema(), schemaTable.getTable());
    }

    @Override
    public List<String> getSplits(String schemaName, String tableName, int maxSplitCount)
    {
        // Todo: this needs TupleDomain to be passed in and desired columns
        Splits splits = defaultApi.schemasSchemaTablesTableSplitsPost(schemaName, tableName,
                new SchemasSchemaTablesTableSplitsPostRequest().maxSplitCount(maxSplitCount));
        return splits.getSplits();
    }
}
