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
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.connector.openapi.clientv3.model.SchemaTable;
import com.facebook.presto.connector.openapi.clientv3.model.TableMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.function.UnaryOperator.identity;

public class OpenAPIMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(OpenAPIMetadata.class);

    // Expire entries in the cache that no longer receive updates
    private static final Duration EXPIRE_AFTER_WRITE = new Duration(10, MINUTES);

    private final OpenAPIService service;
    private final TypeManager typeManager;

    private final LoadingCache<SchemaTableName, Optional<OpenAPITableMetadata>> tableCache;

    @Inject
    public OpenAPIMetadata(
            OpenAPIService service,
            OpenAPIConnectorConfig connectorConfig,
            TypeManager typeManager,
            @ForMetadataRefresh Executor metadataRefreshExecutor)
    {
        this.service = requireNonNull(service);
        this.typeManager = requireNonNull(typeManager);
        this.tableCache = CacheBuilder.newBuilder()
                .expireAfterWrite(EXPIRE_AFTER_WRITE.toMillis(), MILLISECONDS)
                .refreshAfterWrite(connectorConfig.getMetadataRefreshIntervalMs(), MILLISECONDS)
                .build(CacheLoader.asyncReloading(CacheLoader.from(this::fetchTableMetadata), metadataRefreshExecutor));
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
        OpenAPITableHandle tableHandle = (OpenAPITableHandle) table;
        OpenAPITableLayoutHandle layoutHandle = new OpenAPITableLayoutHandle(
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                desiredColumns,
                constraint.getSummary());
        return ImmutableList.of(new ConnectorTableLayoutResult(
                new ConnectorTableLayout(layoutHandle), constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        OpenAPITableHandle tableHandle = (OpenAPITableHandle) table;
        return getRequiredTableMetadata(new SchemaTableName(tableHandle.getSchemaName(), tableHandle.getTableName()));
    }

    private ConnectorTableMetadata getRequiredTableMetadata(SchemaTableName schemaTableName)
    {
        Optional<OpenAPITableMetadata> table = tableCache.getUnchecked(schemaTableName);
        if (!table.isPresent()) {
            throw new TableNotFoundException(schemaTableName);
        }
        else {
            return table.get().toConnectorTableMetadata();
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return getTableMetadata(session, tableHandle).getColumns().stream()
                .collect(ImmutableMap.toImmutableMap(ColumnMetadata::getName, OpenAPIColumnHandle::new));
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        return ((OpenAPIColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        try {
            return service.listTables(schemaName.orElse(null))
                    .stream().map(schemaTable -> new SchemaTableName(schemaTable.getSchema(), schemaTable.getTable()))
                    .collect(toImmutableList());
        }
        catch (OpenAPIServiceException e) {
            throw e.toPrestoException();
        }
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
            ConnectorSession session,
            SchemaTablePrefix prefix)
    {
        try {
            return listTables(session, Optional.ofNullable(prefix.getSchemaName())).stream()
                    .collect(ImmutableMap.toImmutableMap(identity(), schemaTableName -> getRequiredTableMetadata(schemaTableName).getColumns()));
        }
        catch (OpenAPIServiceException e) {
            throw e.toPrestoException();
        }
    }

    private Optional<OpenAPITableMetadata> fetchTableMetadata(SchemaTableName schemaTableName)
    {
        requireNonNull(schemaTableName);

        TableMetadata metadata;
        try {
            SchemaTable schemaTable = new SchemaTable()
                    .schema(schemaTableName.getSchemaName())
                    .table(schemaTableName.getTableName());

            metadata = service.getTableMetadata(schemaTable);
        }
        catch (OpenAPIServiceException e) {
            if (e.getStatusCode() == 404) {
                log.warn(e, "Table not found: %s", schemaTableName);
                return Optional.empty();
            }
            log.warn(e, "Failed to fetch table metadata: %s", schemaTableName);
            throw e.toPrestoException();
        }

        OpenAPITableMetadata tableMetadata = new OpenAPITableMetadata(metadata, typeManager);
        if (!Objects.equals(tableMetadata.getSchemaTableName(), schemaTableName)) {
            throw new PrestoException(OpenAPIErrorCode.OPENAPI_INVALID_RESPONSE, "Request and actual table names are different");
        }

        log.info("Refreshed table metadata: %s", tableMetadata);

        return Optional.of(tableMetadata);
    }
}
