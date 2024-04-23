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
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDefaultOpenAPIService
{
    @Test
    public void testListSchemas() throws Exception
    {
        List<String> expectedSchemas = ImmutableList.of("schema1", "schema2");
        MockResponse response = new MockResponse().setBody(JSON.serialize(expectedSchemas));

        try (MockWebServer httpServer = withMockResponse(response);
                OpenAPIService service = newService(httpServer)) {
            List<String> actualSchemas = service.listSchemaNames();
            assertThat(actualSchemas).isEqualTo(expectedSchemas);
        }
    }

    @Test
    public void testListSchemas_InternalServerError() throws Exception
    {
        Error error = new Error().message("Internal Server Error").retryable(false);
        MockResponse response = new MockResponse().setResponseCode(500).setBody(JSON.serialize(error));

        try (MockWebServer httpServer = withMockResponse(response);
                OpenAPIService service = newService(httpServer)) {
            service.listSchemaNames();
            Assertions.fail("Expected OpenAPIServiceException to be thrown from listSchemaNames");
        }
        catch (OpenAPIServiceException e) {
            assertThat(e.getStatusCode()).isEqualTo(500);
            assertThat(e.getError()).isEqualTo(error);
        }
    }

    @Test
    public void testListTables() throws Exception
    {
        List<SchemaTable> schemaTables = ImmutableList.of(
                new SchemaTable().schema("virtual").table("table1"),
                new SchemaTable().schema("virtual").table("table2"));
        MockResponse response = new MockResponse().setBody(JSON.serialize(schemaTables));

        try (MockWebServer httpServer = withMockResponse(response);
                OpenAPIService service = newService(httpServer)) {
            List<SchemaTable> result = service.listTables("schema");

            Set<String> expectedRefs = schemaTables.stream().map(e -> {
                return e.getSchema() + "." + e.getTable();
            }).collect(Collectors.toSet());

            Set<String> actualRefs = result.stream().map(e -> {
                return e.getSchema() + "." + e.getTable();
            }).collect(Collectors.toSet());

            assertThat(actualRefs).isEqualTo(expectedRefs);
        }
    }

    @Test
    public void testListTables_SchemaNotFound() throws Exception
    {
        Error error = new Error().message("Schema not found").retryable(false);
        MockResponse response = new MockResponse().setResponseCode(404).setBody(JSON.serialize(error));

        try (MockWebServer httpServer = withMockResponse(response);
                OpenAPIService service = newService(httpServer)) {
            service.listTables("schema");
            Assertions.fail("Expected OpenAPIServiceException to be thrown from listTables");
        }
        catch (OpenAPIServiceException e) {
            assertThat(e.getStatusCode()).isEqualTo(404);
            assertThat(e.getError()).isEqualTo(error);
        }
    }

    @Test
    public void testGetTableMetadata() throws Exception
    {
        SchemaTable schemaTable = new SchemaTable().schema("virtual").table("table1");
        TableMetadata expectedMetadata = new TableMetadata()
                .schemaTableName(schemaTable)
                .columns(ImmutableList.of(
                        new ColumnMetadata().name("column1").type("integer"),
                        new ColumnMetadata().name("column2").type("varchar")));
        MockResponse response = new MockResponse().setBody(JSON.serialize(expectedMetadata));

        try (MockWebServer httpServer = withMockResponse(response);
                OpenAPIService service = newService(httpServer)) {
            TableMetadata actualMetadata = service.getTableMetadata(schemaTable);
            assertThat(actualMetadata).isEqualTo(expectedMetadata);
        }
    }

    @Test
    public void testGetTableMetadata_NotFound() throws Exception
    {
        Error error = new Error().message("Table not found").retryable(false);
        MockResponse response = new MockResponse().setResponseCode(404).setBody(JSON.serialize(error));

        try (MockWebServer httpServer = withMockResponse(response);
                OpenAPIService service = newService(httpServer)) {
            service.getTableMetadata(new SchemaTable().schema("virtual").table("table1"));
            Assertions.fail("Expected ApiException to be thrown from getTableMetadata");
        }
        catch (OpenAPIServiceException e) {
            assertThat(e.getStatusCode()).isEqualTo(404);
            assertThat(e.getError()).isEqualTo(error);
        }
    }

    @Test
    public void testGetSplits() throws Exception
    {
        Splits expectedSplits = new Splits().splits(ImmutableList.of("split1", "split2"));
        MockResponse response = new MockResponse().setBody(JSON.serialize(expectedSplits));

        try (MockWebServer httpServer = withMockResponse(response);
                OpenAPIService service = newService(httpServer)) {
            Splits actualSplits = service.getSplits("schema", "table", 10);
            assertThat(actualSplits).isEqualTo(expectedSplits);
        }
    }

    @Test
    public void testGetSplits_NotFound() throws Exception
    {
        Error error = new Error().message("Table not found").retryable(false);
        MockResponse response = new MockResponse().setResponseCode(404).setBody(JSON.serialize(error));

        try (MockWebServer httpServer = withMockResponse(response);
                OpenAPIService service = newService(httpServer)) {
            service.getSplits("schema", "table", 10);
            Assertions.fail("Expected OpenAPIServiceException to be thrown from getSplits");
        }
        catch (OpenAPIServiceException e) {
            assertThat(e.getStatusCode()).isEqualTo(404);
            assertThat(e.getError()).isEqualTo(error);
        }
    }

    @Test
    public void testPageRows() throws Exception
    {
        String rowContent = "row1col1";
        byte[] rowBytes = rowContent.getBytes(StandardCharsets.UTF_8);

        VarcharData varcharData = new VarcharData()
                .addNullsItem(false)
                .addSizesItem(rowBytes.length)
                .bytes(Base64.getEncoder().encodeToString(rowBytes));

        PageResult expectPageResult = new PageResult()
                .rowCount(1)
                .addColumnBlocksItem(new Block().varcharData(varcharData));

        MockResponse response = new MockResponse().setBody(JSON.serialize(expectPageResult));

        try (MockWebServer httpServer = withMockResponse(response);
                OpenAPIService service = newService(httpServer)) {
            PageResult actualPageResult = service.getPageRows("schema",
                    "table",
                    "split",
                    ImmutableList.of("column1"),
                    null,
                    null);

            assertThat(actualPageResult).isEqualTo(expectPageResult);
        }
    }

    @Test
    public void testPageRows_NotFound() throws Exception
    {
        Error error = new Error().message("Table not found").retryable(false);
        MockResponse response = new MockResponse().setResponseCode(404).setBody(JSON.serialize(error));

        try (MockWebServer httpServer = withMockResponse(response);
                OpenAPIService service = newService(httpServer)) {
            service.getPageRows("schema", "table", "split",
                    ImmutableList.of("column1"), null, null);
            Assertions.fail("Expected OpenAPIServiceException to be thrown from getPageRows");
        }
        catch (OpenAPIServiceException e) {
            assertThat(e.getStatusCode()).isEqualTo(404);
            assertThat(e.getError()).isEqualTo(error);
        }
    }

    private MockWebServer withMockResponse(MockResponse... responses)
    {
        MockWebServer httpServer = new MockWebServer();
        try {
            httpServer.start();
            for (MockResponse response : responses) {
                httpServer.enqueue(response);
            }
            return httpServer;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private OpenAPIService newService(MockWebServer httpServer)
    {
        OpenAPIConnectorConfig config = new OpenAPIConnectorConfig();
        config.setBaseUrl(httpServer.url("/").toString());
        return new DefaultOpenAPIService(config);
    }
}
