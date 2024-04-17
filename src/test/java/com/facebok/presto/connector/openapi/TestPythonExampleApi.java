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

import com.facebook.presto.connector.openapi.clientv3.ApiClient;
import com.facebook.presto.connector.openapi.clientv3.Configuration;
import com.facebook.presto.connector.openapi.clientv3.api.DefaultApi;
import com.facebook.presto.connector.openapi.clientv3.model.ColumnMetadata;
import com.facebook.presto.connector.openapi.clientv3.model.PageResult;
import com.facebook.presto.connector.openapi.clientv3.model.SchemaTable;
import com.facebook.presto.connector.openapi.clientv3.model.SchemasSchemaTablesTableSplitsPostRequest;
import com.facebook.presto.connector.openapi.clientv3.model.SchemasSchemaTablesTableSplitsSplitRowsPostRequest;
import com.facebook.presto.connector.openapi.clientv3.model.Splits;
import com.facebook.presto.connector.openapi.clientv3.model.TableMetadata;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestPythonExampleApi
{
    private DefaultApi defaultApi;

    @BeforeClass
    public void setup()
    {
        if (!isLocalTestServerRunning()) {
            throw new SkipException("Local server is not running. Skipping tests.");
        }
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost:8080");
        defaultApi = new DefaultApi(defaultClient);
    }

    @Test
    public void testListSchemas()
    {
        List<String> schemas = defaultApi.schemasGet();
        assertEquals(schemas.size(), 2);
        assertTrue(schemas.contains("sales"));
        assertTrue(schemas.contains("inventory"));
    }

    @Test
    public void testListTables()
    {
        List<SchemaTable> tables = defaultApi.schemasSchemaTablesGet("sales");
        assertEquals(tables.size(), 2);

        Set<String> actual = new HashSet<>();
        for (SchemaTable table : tables) {
            actual.add(table.getSchema() + "." + table.getTable());
        }

        assertTrue(actual.contains("sales.orders"));
        assertTrue(actual.contains("sales.customers"));
    }

    @Test
    public void testGetTableMetadata()
    {
        TableMetadata metadata = defaultApi.schemasSchemaTablesTableGet("sales", "orders");
        assertEquals(metadata.getSchemaTableName().getSchema(), "sales");
        assertEquals(metadata.getSchemaTableName().getTable(), "orders");
        assertEquals(metadata.getColumns().size(), 4);
        assertEquals(metadata.getColumns().get(0).getName(), "order_id");
        assertEquals(metadata.getColumns().get(0).getType(), "varchar");
        assertNull(metadata.getColumns().get(0).getComment());
        assertNull(metadata.getComment());
    }

    @Test
    public void testGetSplitsAndRowsWithAllColumns()
    {
        int maxSplitCount = 50;

        List<String> allColumns = extractColumnNames(defaultApi.schemasSchemaTablesTableGet("sales", "orders"));
        allColumns.remove("order_date");

        // Get a batch of splits
        SchemasSchemaTablesTableSplitsPostRequest splitsRequestBody = new SchemasSchemaTablesTableSplitsPostRequest()
                .desiredColumns(allColumns)
                .maxSplitCount(maxSplitCount);
        Splits splits = defaultApi.schemasSchemaTablesTableSplitsPost("sales", "orders", splitsRequestBody);

        // Test each split in the batch
        for (String split : splits.getSplits()) {
            String nextToken = null;
            int rowCount = 0;
            do {
                SchemasSchemaTablesTableSplitsSplitRowsPostRequest requestBody = new SchemasSchemaTablesTableSplitsSplitRowsPostRequest()
                        .desiredColumns(allColumns).nextToken(nextToken);
                PageResult rowData = defaultApi.schemasSchemaTablesTableSplitsSplitRowsPost("sales",
                        "orders",
                        split,
                        requestBody);

                assertEquals(rowData.getColumnBlocks().size(), allColumns.size());

                rowCount += rowData.getRowCount();
                nextToken = rowData.getNextToken();
            } while (nextToken != null);
            assertEquals(rowCount, 5, "Expected 5 rows in split " + split);
        }

        // Check if all splits cover the entire dataset
        assertEquals(splits.getSplits().size(), 6);
    }

    private List<String> extractColumnNames(TableMetadata tableMetadata)
    {
        List<String> columnNames = new java.util.ArrayList<>();
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            columnNames.add(column.getName());
        }
        return columnNames;
    }

    private boolean isLocalTestServerRunning()
    {
        try {
            URL url = new URL("http://localhost:8080/schemas");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(1000); // Timeout in milliseconds
            connection.connect();
            return connection.getResponseCode() == HttpURLConnection.HTTP_OK;
        }
        catch (Exception e) {
            return false;
        }
    }
}
