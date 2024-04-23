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

import com.facebook.presto.connector.openapi.clientv3.JSON;
import com.facebook.presto.connector.openapi.clientv3.model.Block;
import com.facebook.presto.connector.openapi.clientv3.model.ColumnMetadata;
import com.facebook.presto.connector.openapi.clientv3.model.Error;
import com.facebook.presto.connector.openapi.clientv3.model.PageResult;
import com.facebook.presto.connector.openapi.clientv3.model.SchemaTable;
import com.facebook.presto.connector.openapi.clientv3.model.Splits;
import com.facebook.presto.connector.openapi.clientv3.model.TableMetadata;
import com.facebook.presto.connector.openapi.clientv3.model.VarcharData;
import com.google.common.collect.ImmutableList;
import com.ning.http.util.Base64;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.assertj.core.api.Assertions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDefaultOpenAPIService
{
    private MockWebServer httpServer;
    private DefaultOpenAPIService service;

    @BeforeClass
    public void setupClass() throws IOException
    {
        httpServer = new MockWebServer();
        httpServer.start();

        OpenAPIConnectorConfig config = new OpenAPIConnectorConfig();
        config.setBaseUrl(httpServer.url("/").toString());

        service = new DefaultOpenAPIService(config);
    }

    @AfterClass
    public void teardownClass() throws IOException
    {
        httpServer.shutdown();
    }

    @Test
    public void testListSchemas()
    {
        List<String> expectedSchemas = ImmutableList.of("schema1", "schema2");

        httpServer.enqueue(new MockResponse().setBody(JSON.serialize(expectedSchemas)));
        List<String> actualSchemas = service.listSchemaNames();

        assertThat(actualSchemas).isEqualTo(expectedSchemas);
    }

    @Test
    public void testListSchemas_InternalServerError()
    {
        Error error = new Error().message("Internal Server Error").retryable(false);
        httpServer.enqueue(new MockResponse().setResponseCode(500).setBody(JSON.serialize(error)));

        try {
            service.listSchemaNames();
            Assertions.fail("Expected ApiException to be thrown from listSchemaNames");
        }
        catch (OpenAPIServiceException e) {
            assertThat(e.getStatusCode()).isEqualTo(500);
            assertThat(e.getError()).isEqualTo(error);
        }
    }

    @Test
    public void testListTables()
    {
        List<SchemaTable> schemaTables = ImmutableList.of(
                new SchemaTable().schema("virtual").table("table1"),
                new SchemaTable().schema("virtual").table("table2"));

        httpServer.enqueue(new MockResponse().setBody(JSON.serialize(schemaTables)));
        List<SchemaTable> result = service.listTables("schema");

        Set<String> expectedRefs = schemaTables.stream().map(e -> {
            return e.getSchema() + "." + e.getTable();
        }).collect(Collectors.toSet());

        Set<String> actualRefs = result.stream().map(e -> {
            return e.getSchema() + "." + e.getTable();
        }).collect(Collectors.toSet());

        assertThat(actualRefs).isEqualTo(expectedRefs);
    }

    @Test
    public void testListTables_SchemaNotFound()
    {
        Error error = new Error().message("Schema not found").retryable(false);
        httpServer.enqueue(new MockResponse().setResponseCode(404).setBody(JSON.serialize(error)));

        try {
            service.listTables("schema");
            Assertions.fail("Expected ApiException to be thrown from listTables");
        }
        catch (OpenAPIServiceException e) {
            assertThat(e.getStatusCode()).isEqualTo(404);
            assertThat(e.getError()).isEqualTo(error);
        }
    }

    @Test
    public void testGetTableMetadata()
    {
        SchemaTable schemaTable = new SchemaTable().schema("virtual").table("table1");
        TableMetadata expectedMetadata = new TableMetadata()
                .schemaTableName(schemaTable)
                .columns(ImmutableList.of(
                        new ColumnMetadata().name("column1").type("integer"),
                        new ColumnMetadata().name("column2").type("varchar")));
        httpServer.enqueue(new MockResponse().setBody(JSON.serialize(expectedMetadata)));
        TableMetadata actualMetadata = service.getTableMetadata(schemaTable);

        assertThat(actualMetadata).isEqualTo(expectedMetadata);
    }

    @Test
    public void testGetTableMetadata_NotFound()
    {
        Error error = new Error().message("Table not found").retryable(false);
        httpServer.enqueue(new MockResponse().setResponseCode(404).setBody(JSON.serialize(error)));

        try {
            service.getTableMetadata(new SchemaTable().schema("virtual").table("table1"));
            Assertions.fail("Expected ApiException to be thrown from getTableMetadata");
        }
        catch (OpenAPIServiceException e) {
            assertThat(e.getStatusCode()).isEqualTo(404);
            assertThat(e.getError()).isEqualTo(error);
        }
    }

    @Test
    public void testGetSplits()
    {
        Splits expectedSplits = new Splits().splits(ImmutableList.of("split1", "split2"));
        httpServer.enqueue(new MockResponse().setBody(JSON.serialize(expectedSplits)));

        Splits actualSplits = service.getSplits("schema", "table", 10);
        assertThat(actualSplits).isEqualTo(expectedSplits);
    }

    @Test
    public void testGetSplits_NotFound()
    {
        Error error = new Error().message("Table not found").retryable(false);
        httpServer.enqueue(new MockResponse().setResponseCode(404).setBody(JSON.serialize(error)));

        try {
            service.getSplits("schema", "table", 10);
            Assertions.fail("Expected ApiException to be thrown from getSplits");
        }
        catch (OpenAPIServiceException e) {
            assertThat(e.getStatusCode()).isEqualTo(404);
            assertThat(e.getError()).isEqualTo(error);
        }
    }

    @Test
    public void testPageRows()
    {
        String rowContent = "row1col1";
        byte[] rowBytes = rowContent.getBytes(StandardCharsets.UTF_8);

        VarcharData varcharData = new VarcharData()
                .addNullsItem(false)
                .addSizesItem(rowBytes.length)
                .bytes(Base64.encode(rowBytes));

        PageResult expectPageResult = new PageResult()
                .rowCount(1)
                .addColumnBlocksItem(new Block().varcharData(varcharData));

        httpServer.enqueue(new MockResponse().setBody(JSON.serialize(expectPageResult)));

        PageResult actualPageResult = service.getPageRows("schema",
                "table",
                "split",
                ImmutableList.of("column1"),
                null,
                null);

        assertThat(actualPageResult).isEqualTo(expectPageResult);
    }
}
