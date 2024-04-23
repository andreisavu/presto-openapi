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

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.assertj.core.api.Assertions;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

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
        httpServer.enqueue(new MockResponse().setBody("[\"schema1\", \"schema2\"]"));

        List<String> schemas = service.listSchemaNames();
        Assertions.assertThat(schemas).containsExactly("schema1", "schema2");
    }
}
