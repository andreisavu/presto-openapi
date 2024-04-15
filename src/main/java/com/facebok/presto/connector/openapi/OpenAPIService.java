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

import com.facebook.presto.connector.openapi.clientv3.model.SchemaTable;
import com.facebook.presto.connector.openapi.clientv3.model.TableMetadata;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.List;

public interface OpenAPIService
{
    /**
     * Returns the base URI of the OpenAPI service.
     */
    URI getBaseURI();

    /**
     * Returns available schema names
     */
    List<String> listSchemaNames();

    /**
     * Returns the tables for a given schema name.
     *
     * @param schemaOrNull the schema name or {@literal null}
     * @return a list of tables names with schemas. If the schema name is null then returns
     * a list of tables for all schemas.
     */
    List<SchemaTable> listTables(@Nullable String schemaOrNull);

    /**
     * Returns metadata for a given table.
     *
     * @param schemaTable schema and table name
     * @return metadata for a given table, or a {@literal null} value inside if it does not exist
     */
    TableMetadata getTableMetadata(SchemaTable schemaTable);

    /**
     * Returns a list of splits for a given table.
     *
     * @param schemaName schema name
     * @param tableName table name
     * @param maxSplitCount maximum number of splits to return
     * @return a list of splits
     */
    List<String> getSplits(String schemaName, String tableName, int maxSplitCount);

    // TODO: handle rows, projections, and constraints
}
