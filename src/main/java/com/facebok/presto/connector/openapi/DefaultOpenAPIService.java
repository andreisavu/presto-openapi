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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.connector.openapi.clientv3.ApiClient;
import com.facebook.presto.connector.openapi.clientv3.ApiException;
import com.facebook.presto.connector.openapi.clientv3.Configuration;
import com.facebook.presto.connector.openapi.clientv3.api.DefaultApi;
import com.facebook.presto.connector.openapi.clientv3.auth.ApiKeyAuth;
import com.facebook.presto.connector.openapi.clientv3.model.PageResult;
import com.facebook.presto.connector.openapi.clientv3.model.SchemaTable;
import com.facebook.presto.connector.openapi.clientv3.model.SchemasSchemaTablesTableSplitsPostRequest;
import com.facebook.presto.connector.openapi.clientv3.model.SchemasSchemaTablesTableSplitsSplitRowsPostRequest;
import com.facebook.presto.connector.openapi.clientv3.model.Splits;
import com.facebook.presto.connector.openapi.clientv3.model.TableMetadata;
import com.facebook.presto.connector.openapi.clientv3.model.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class DefaultOpenAPIService
        implements OpenAPIService
{
    private static final Logger log = Logger.get(DefaultOpenAPIService.class);

    private final DefaultApi defaultApi;
    private final URI baseURI;

    @Inject
    DefaultOpenAPIService(OpenAPIConnectorConfig config)
    {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath(config.getBaseUrl());
        log.info("Using base URL: %s", config.getBaseUrl());

        // Set up authentication if needed (bearer token, basic auth or API key)
        if (config.getBearerToken() != null) {
            defaultClient.setBearerToken(config.getBearerToken());
            log.info("Using bearer token for authentication (token length: %d)", config.getBearerToken().length());
        }
        else if (config.getBasicAuthUsername() != null && config.getBasicAuthPassword() != null) {
            defaultClient.setUsername(config.getBasicAuthUsername());
            defaultClient.setPassword(config.getBasicAuthPassword());
            log.info("Using basic auth for authentication (username: %s)", config.getBasicAuthUsername());
        }
        else if (config.getApiKey() != null) {
            defaultClient.setApiKey(config.getApiKey());
            ApiKeyAuth apiKeyAuth = (ApiKeyAuth) defaultClient.getAuthentication("ApiKeyAuth");
            log.info("Using API key for authentication as the %s header (key length: %s)",
                    apiKeyAuth.getParamName(), config.getApiKey().length());
        }

        defaultClient.setConnectTimeout(config.getHttpClientConnectTimeoutMs());
        defaultClient.setReadTimeout(config.getHttpClientReadTimeoutMs());
        defaultClient.setWriteTimeout(config.getHttpClientWriteTimeoutMs());

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
        try {
            List<String> schemas = defaultApi.schemasGet();
            return ImmutableList.copyOf(schemas);
        }
        catch (ApiException e) {
            log.error(e, "Failed to list schemas");
            throw new OpenAPIServiceException(e);
        }
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
            try {
                result.addAll(defaultApi.schemasSchemaTablesGet(schemaName));
            }
            catch (ApiException e) {
                log.error(e, "Failed to list tables for schema: %s", schemaName);
                throw new OpenAPIServiceException(e);
            }
        }
        return ImmutableList.copyOf(result);
    }

    @Override
    public TableMetadata getTableMetadata(SchemaTable schemaTable)
    {
        try {
            return defaultApi.schemasSchemaTablesTableGet(schemaTable.getSchema(), schemaTable.getTable());
        }
        catch (ApiException e) {
            log.error(e, "Failed to get metadata for table: %s.%s", schemaTable.getSchema(), schemaTable.getTable());
            throw new OpenAPIServiceException(e);
        }
    }

    @Override
    public Splits getSplits(String schemaName, String tableName, int maxSplitCount)
    {
        SchemasSchemaTablesTableSplitsPostRequest requestBody = new SchemasSchemaTablesTableSplitsPostRequest()
                .maxSplitCount(maxSplitCount);
        try {
            return defaultApi.schemasSchemaTablesTableSplitsPost(schemaName, tableName, requestBody);
        }
        catch (ApiException e) {
            log.error(e, "Failed to get splits for table: %s.%s", schemaName, tableName);
            throw new OpenAPIServiceException(e);
        }
    }

    @Override
    public PageResult getPageRows(String schemaName,
                                  String tableName,
                                  String split,
                                  List<String> desiredColumns,
                                  TupleDomain outputConstraint,
                                  @Nullable String nextToken)
    {
        SchemasSchemaTablesTableSplitsSplitRowsPostRequest requestBody = new SchemasSchemaTablesTableSplitsSplitRowsPostRequest()
                .desiredColumns(desiredColumns)
                .outputConstraint(outputConstraint)
                .nextToken(nextToken);

        try {
            return defaultApi.schemasSchemaTablesTableSplitsSplitRowsPost(schemaName,
                    tableName,
                    split,
                    requestBody);
        }
        catch (ApiException e) {
            log.error(e, "Failed to get rows for table: %s.%s", schemaName, tableName);
            throw new OpenAPIServiceException(e);
        }
    }

    @Override
    public void close()
    {
    }
}
