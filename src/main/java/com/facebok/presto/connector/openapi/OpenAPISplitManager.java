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

import com.facebook.presto.connector.openapi.clientv3.model.Splits;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class OpenAPISplitManager
        implements ConnectorSplitManager
{
    private static final int DEFAULT_MAX_SPLIT_COUNT = 128;

    private final OpenAPIService service;

    @Inject
    public OpenAPISplitManager(OpenAPIService service)
    {
        this.service = requireNonNull(service);
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        OpenAPITableLayoutHandle tableHandle = (OpenAPITableLayoutHandle) layout;

        Splits splits = service.getSplits(tableHandle.getSchemaName(),
                tableHandle.getTableName(), DEFAULT_MAX_SPLIT_COUNT);

        List<ConnectorSplit> result = splits.getSplits().stream()
                .map(split -> new OpenAPIConnectorSplit(tableHandle.getSchemaName(),
                        tableHandle.getTableName(),
                        split,
                        service.getBaseURI()))
                .collect(ImmutableList.toImmutableList());

        return new FixedSplitSource(result);
    }
}
