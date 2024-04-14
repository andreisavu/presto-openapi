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
import com.facebook.presto.connector.openapi.clientv3.model.PageResult;
import com.facebook.presto.connector.openapi.clientv3.model.SchemaTable;
import com.facebook.presto.connector.openapi.clientv3.model.SchemasSchemaTablesTableSplitsPostRequest;
import com.facebook.presto.connector.openapi.clientv3.model.Split;
import com.facebook.presto.connector.openapi.clientv3.model.SplitBatch;
import com.facebook.presto.connector.openapi.clientv3.model.SplitsSplitIdRowsPostRequest;
import com.facebook.presto.connector.openapi.clientv3.model.TableMetadata;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

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
        assertEquals(tables.get(0).getSchema(), "sales");
        assertEquals(tables.get(0).getTable(), "orders");
        assertEquals(tables.get(1).getSchema(), "sales");
        assertEquals(tables.get(1).getTable(), "customers");
    }

    @Test
    public void testGetTableMetadata()
    {
        TableMetadata metadata = defaultApi.schemasSchemaTablesTableGet("sales", "orders");
        assertEquals(metadata.getSchemaTableName().getSchema(), "sales");
        assertEquals(metadata.getSchemaTableName().getTable(), "orders");
        assertEquals(metadata.getColumns().size(), 4);
        assertEquals(metadata.getColumns().get(0).getName(), "order_id");
        assertEquals(metadata.getColumns().get(0).getType(), "string");
        assertNull(metadata.getColumns().get(0).getComment());
        // Assert the remaining columns
        assertNull(metadata.getComment());
        assertEquals(metadata.getIndexableKeys().size(), 0);
    }

    @Test
    public void testGetSplitsAndRows()
    {
        int maxSplitCount = 5;
        String splitsNextToken = null;
        int totalSplits = 0;

        do {
            // Get a batch of splits
            SplitBatch splitBatch = defaultApi.schemasSchemaTablesTableSplitsPost("sales", "orders",
                    new SchemasSchemaTablesTableSplitsPostRequest().maxSplitCount(maxSplitCount).nextToken(splitsNextToken));
            totalSplits += splitBatch.getSplits().size();
            splitsNextToken = splitBatch.getNextToken();

            // Test each split in the batch
            for (Split split : splitBatch.getSplits()) {
                String nextToken = null;
                do {
                    PageResult rowData = defaultApi.splitsSplitIdRowsPost(split.getSplitId(),
                            new SplitsSplitIdRowsPostRequest().nextToken(nextToken));
                    assertEquals(rowData.getRowCount().intValue(), 1);
                    nextToken = rowData.getNextToken();
                } while (nextToken != null);
            }
        } while (splitsNextToken != null);

        // Check if all splits cover the entire dataset
        assertEquals(totalSplits, 30);
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
