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

import com.facebok.presto.connector.openapi.annotations.ForMetadataRefresh;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.units.Duration;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class OpenAPIMetadata
        implements ConnectorMetadata
{
    private static final Duration EXPIRE_AFTER_WRITE = new Duration(10, MINUTES);
    private static final Duration REFRESH_AFTER_WRITE = new Duration(2, MINUTES);

    private final OpenAPIService service;
    private final LoadingCache<SchemaTableName, Optional<OpenAPITableMetadata>> tableCache;

    @Inject
    public OpenAPIMetadata(
            OpenAPIService service,
            @ForMetadataRefresh Executor metadataRefreshExecutor)
    {
        this.service = requireNonNull(service);
        tableCache = CacheBuilder.newBuilder()
                .expireAfterWrite(EXPIRE_AFTER_WRITE.toMillis(), MILLISECONDS)
                .refreshAfterWrite(REFRESH_AFTER_WRITE.toMillis(), MILLISECONDS)
                .build(CacheLoader.asyncReloading(CacheLoader.from(this::fetchTableMetadata), metadataRefreshExecutor));
    }

    private Optional<OpenAPITableMetadata> fetchTableMetadata(SchemaTableName schemaTableName)
    {
        return Optional.empty();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return service.listSchemaNames();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return tableCache.getUnchecked(tableName)
                .map(OpenAPITableMetadata::getSchemaTableName)
                .map(OpenAPITableHandle::new)
                .orElse(null);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        return ImmutableList.of();
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return null;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        return null;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return ImmutableMap.of();
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        return null;
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
            ConnectorSession session,
            SchemaTablePrefix prefix)
    {
        return ImmutableMap.of();
    }
}
