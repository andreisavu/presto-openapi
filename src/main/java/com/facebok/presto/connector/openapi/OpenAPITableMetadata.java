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

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.connector.openapi.clientv3.model.TableMetadata;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class OpenAPITableMetadata
{
    private final SchemaTableName schemaTableName;
    private final Optional<String> comment;
    private final List<ColumnMetadata> columns;

    public OpenAPITableMetadata(TableMetadata metadata, TypeManager typeManager)
    {
        requireNonNull(metadata.getSchemaTableName());

        String schemaName = requireNonNull(metadata.getSchemaTableName().getSchema());
        String tableName = requireNonNull(metadata.getSchemaTableName().getTable());

        this.schemaTableName = new SchemaTableName(schemaName, tableName);
        this.comment = Optional.ofNullable(metadata.getComment());
        this.columns = extractColumnMetadata(metadata, typeManager);
    }

    private List<ColumnMetadata> extractColumnMetadata(TableMetadata metadata, TypeManager typeManager)
    {
        ImmutableList.Builder<ColumnMetadata> result = ImmutableList.builder();
        for (com.facebook.presto.connector.openapi.clientv3.model.ColumnMetadata current : metadata.getColumns()) {
            ColumnMetadata element = new ColumnMetadata(
                    current.getName(),
                    typeManager.getType(TypeSignature.parseTypeSignature(current.getType())));
            result.add(element);
        }
        return result.build();
    }

    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    public Optional<String> getComment()
    {
        return comment;
    }

    public List<ColumnMetadata> getColumns()
    {
        return columns;
    }

    public ConnectorTableMetadata toConnectorTableMetadata()
    {
        return new ConnectorTableMetadata(
                schemaTableName,
                columns,
                ImmutableMap.of(),
                comment);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        OpenAPITableMetadata other = (OpenAPITableMetadata) obj;
        return Objects.equals(this.schemaTableName, other.schemaTableName) &&
                Objects.equals(this.columns, other.columns) &&
                Objects.equals(this.comment, other.comment);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName, columns, comment);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaTableName", schemaTableName)
                .add("columns", columns)
                .add("comment", comment)
                .toString();
    }
}
