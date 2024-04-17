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

import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Locale;
import java.util.Objects;
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

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OpenAPITableLayoutHandle that = (OpenAPITableLayoutHandle) o;
        return Objects.equals(schemaName, that.schemaName)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(desiredColumns, that.desiredColumns)
                && Objects.equals(constraint, that.constraint);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, desiredColumns, constraint);
    }

    @Override
    public String toString()
    {
        // Somewhat of a hack needed in order to get a useful string representation of the constraint
        SqlFunctionProperties props = SqlFunctionProperties.builder()
                .setParseDecimalLiteralAsDouble(false)
                .setLegacyRowFieldOrdinalAccessEnabled(false)
                .setTimeZoneKey(TimeZoneKey.UTC_KEY)
                .setLegacyTimestamp(false)
                .setLegacyMapSubscript(false)
                .setSessionStartTime(0)
                .setSessionLocale(Locale.US)
                .setSessionUser("toString()")
                .setFieldNamesInJsonCastEnabled(false)
                .build();
        return "OpenAPITableLayoutHandle{" +
                "schemaName='" + schemaName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", desiredColumns=" + desiredColumns +
                ", constraint=" + constraint.toString(props) +
                '}';
    }
}
