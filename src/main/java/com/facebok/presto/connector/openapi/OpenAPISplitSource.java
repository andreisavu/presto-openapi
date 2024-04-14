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

import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public class OpenAPISplitSource
        implements ConnectorSplitSource
{
    private final OpenAPIService service;

    private final String schemaName;
    private final String tableName;

    private final List<String> desiredColumns;
    private volatile boolean isFinished;

    public OpenAPISplitSource(OpenAPIService service, OpenAPITableLayoutHandle tableHandle)
    {
        this.service = requireNonNull(service);
        this.schemaName = requireNonNull(tableHandle.getSchemaName());
        this.tableName = requireNonNull(tableHandle.getTableName());
        this.desiredColumns = tableHandle.getDesiredColumnNames();
        // TODO: add support for TupleDomain
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        isFinished = true;
        return CompletableFuture.completedFuture(new ConnectorSplitBatch(ImmutableList.of(), true));
    }

    @Override
    public void close()
    {
        // Nothing to do here
    }

    @Override
    public boolean isFinished()
    {
        return isFinished;
    }
}
