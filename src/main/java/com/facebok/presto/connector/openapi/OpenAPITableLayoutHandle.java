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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class OpenAPITableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final String schemaName;
    private final String tableName;
    private final Optional<Set<ColumnHandle>> desiredColumns;
    private final TupleDomain<ColumnHandle> constraint;

    @JsonCreator
    public OpenAPITableLayoutHandle(@JsonProperty("schemaName") String schemaName,
                                    @JsonProperty("tableName") String tableName,
                                    @JsonProperty("desiredColumns") Optional<Set<ColumnHandle>> desiredColumns,
                                    @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint)
    {
        this.schemaName = requireNonNull(schemaName);
        this.tableName = requireNonNull(tableName);
        this.desiredColumns = requireNonNull(desiredColumns);
        this.constraint = requireNonNull(constraint);
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public Optional<Set<ColumnHandle>> getDesiredColumns()
    {
        return desiredColumns;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    public List<String> getDesiredColumnNames()
    {
        return desiredColumns.map(columnHandles -> {
            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            columnHandles.forEach(columnHandle ->
                    columnNames.add(((OpenAPIColumnHandle) columnHandle).getColumnMetadata().getName()));
            return columnNames.build();
        }).orElse(ImmutableList.of());
    }
}
