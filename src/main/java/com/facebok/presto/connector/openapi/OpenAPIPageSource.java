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
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.VariableWidthBlock;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Ranges;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.connector.openapi.clientv3.model.EquatableValueSet;
import com.facebook.presto.connector.openapi.clientv3.model.PageResult;
import com.facebook.presto.connector.openapi.clientv3.model.ValueSet;
import com.facebook.presto.connector.openapi.clientv3.model.VarcharData;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class OpenAPIPageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(OpenAPIPageSource.class);

    private static final TypeSignature VARCHAR_TYPE_SIGNATURE = TypeSignature.parseTypeSignature("varchar");

    private final OpenAPIService service;
    private final OpenAPIConnectorSplit split;

    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final com.facebook.presto.connector.openapi.clientv3.model.TupleDomain outputConstraint;

    private final AtomicLong readTimeNanos = new AtomicLong(0);

    private String nextToken;
    private boolean firstCall = true;

    private long completedBytes;
    private long completedPositions;

    public OpenAPIPageSource(OpenAPIService service,
                             OpenAPIConnectorSplit split,
                             List<ColumnHandle> columns,
                             TupleDomain<ColumnHandle> constraints)
    {
        this.service = requireNonNull(service);
        this.split = requireNonNull(split);

        requireNonNull(columns, "columns is null");
        ImmutableList.Builder<String> columnNames = new ImmutableList.Builder<>();
        ImmutableList.Builder<Type> columnTypes = new ImmutableList.Builder<>();
        for (ColumnHandle columnHandle : columns) {
            OpenAPIColumnHandle thriftColumnHandle = (OpenAPIColumnHandle) columnHandle;
            columnNames.add(thriftColumnHandle.getName());
            columnTypes.add(thriftColumnHandle.getType());
        }
        this.columnNames = columnNames.build();
        this.columnTypes = columnTypes.build();
        this.outputConstraint = convertToOpenAPITupleDomain(constraints);
    }

    private com.facebook.presto.connector.openapi.clientv3.model.TupleDomain convertToOpenAPITupleDomain(
            TupleDomain<ColumnHandle> constraints)
    {
        if (constraints == null) {
            return null;
        }

        Map<String, com.facebook.presto.connector.openapi.clientv3.model.Domain> openAPIDomains = new HashMap<>();
        constraints.getDomains().ifPresent(domains -> {
            for (ColumnHandle columnHandle : domains.keySet()) {
                String columnName = ((OpenAPIColumnHandle) columnHandle).getName();
                Type columnType = ((OpenAPIColumnHandle) columnHandle).getType();
                Domain domain = domains.get(columnHandle);

                if (!columnType.getTypeSignature().equals(VARCHAR_TYPE_SIGNATURE) ||
                        !domain.getType().getTypeSignature().equals(VARCHAR_TYPE_SIGNATURE)) {
                    continue;   // Skip non-VARCHAR columns
                }
                if (domain.isSingleValue()) {
                    Slice value = (Slice) domain.getSingleValue();
                    transformSingleValue(value, false, openAPIDomains, columnName);
                }
                else if (domain.isNullAllowed() && domain.getValues().isSingleValue()) {
                    Ranges ranges = domain.getValues().getRanges();
                    Slice value = (Slice) ranges.getSpan().getSingleValue();
                    transformSingleValue(value, true, openAPIDomains, columnName);
                }
                // Not being able to handle a domain is not an error. It's up to the backend to
                // decide how to handle the missing domain if it's important for the query.
            }
        });

        return new com.facebook.presto.connector.openapi.clientv3.model.TupleDomain()
                .domains(openAPIDomains);
    }

    private static void transformSingleValue(Slice value,
                                             boolean nullAllowed,
                                             Map<String, com.facebook.presto.connector.openapi.clientv3.model.Domain> openAPIDomains,
                                             String columnName)
    {
        String valueBase64 = Base64.getEncoder().encodeToString(value.getBytes());

        VarcharData wordVarcharData = new VarcharData()
                .nulls(ImmutableList.of(false))
                .sizes(ImmutableList.of(value.length()))
                .bytes(valueBase64);

        ValueSet equatable = new ValueSet()
                .equatable(new EquatableValueSet()
                        .values(ImmutableList.of(
                                new com.facebook.presto.connector.openapi.clientv3.model.Block().varcharData(wordVarcharData))));

        openAPIDomains.put(columnName, new com.facebook.presto.connector.openapi.clientv3.model.Domain()
                .nullAllowed(nullAllowed).valueSet(equatable));
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getCompletedPositions()
    {
        return completedPositions;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos.get();
    }

    @Override
    public boolean isFinished()
    {
        return !firstCall && nextToken == null;
    }

    @Override
    public Page getNextPage()
    {
        PageResult pageResult = service.getPageRows(split.getSchemaName(),
                split.getTableName(),
                split.getSplit(),
                columnNames,
                outputConstraint,
                nextToken);

        firstCall = false;
        nextToken = pageResult.getNextToken();

        Page page = toPage(pageResult);
        if (page != null) {
            long pageSize = page.getSizeInBytes();
            completedBytes += pageSize;
            completedPositions += page.getPositionCount();
        }
        return page;
    }

    private Page toPage(PageResult pageResult)
    {
        if (pageResult == null || pageResult.getRowCount() == null || pageResult.getRowCount() == 0) {
            return null;
        }
        int numberOfColumns = requireNonNull(pageResult.getColumnBlocks()).size();
        checkArgument(numberOfColumns == columnTypes.size(),
                "columns and type size mismatch in response");
        if (numberOfColumns == 0) {
            // request/response with no columns, used for queries like "select count star"
            return new Page(pageResult.getRowCount());
        }

        Block[] blocks = new Block[numberOfColumns];
        for (int i = 0; i < numberOfColumns; i++) {
            blocks[i] = toPageBlock(pageResult.getColumnBlocks().get(i), columnTypes.get(i));
        }
        return new Page(blocks);
    }

    private Block toPageBlock(
            com.facebook.presto.connector.openapi.clientv3.model.Block block,
            Type columnType)
    {
        if (columnType.getTypeSignature().equals(VARCHAR_TYPE_SIGNATURE)) {
            int numberOfRecords = block.getVarcharData().getSizes().size();

            // Copy the array of nulls flags
            boolean[] nulls = new boolean[numberOfRecords];
            for (int i = 0; i < numberOfRecords; i++) {
                nulls[i] = block.getVarcharData().getNulls().get(i);
            }

            // Extract the array of sizes
            int[] sizes = new int[numberOfRecords];
            for (int i = 0; i < numberOfRecords; i++) {
                sizes[i] = block.getVarcharData().getSizes().get(i);
            }

            // Extract the array of bytes
            byte[] bytes = Base64.getDecoder().decode(block.getVarcharData().getBytes());
            Slice values = Slices.wrappedBuffer(bytes);

            return new VariableWidthBlock(
                    numberOfRecords,
                    values,
                    calculateOffsets(sizes, nulls, numberOfRecords),
                    Optional.ofNullable(nulls));
        }
        else {
            throw new PrestoException(
                    OpenAPIErrorCode.OPENAPI_NOT_IMPLEMENTED,
                    "Unsupported column type: " + columnType.getTypeSignature());
        }
    }

    public static int[] calculateOffsets(int[] sizes, boolean[] nulls, int totalRecords)
    {
        if (sizes == null) {
            return new int[totalRecords + 1];
        }
        int[] offsets = new int[totalRecords + 1];
        offsets[0] = 0;
        for (int i = 0; i < totalRecords; i++) {
            int size = nulls != null && nulls[i] ? 0 : sizes[i];
            offsets[i + 1] = offsets[i] + size;
        }
        return offsets;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close() throws IOException
    {
    }
}
