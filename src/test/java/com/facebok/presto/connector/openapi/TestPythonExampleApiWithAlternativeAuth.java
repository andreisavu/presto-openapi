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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestPythonExampleApiWithAlternativeAuth
{
    @BeforeClass
    public void setupClass()
    {
        TestPythonExampleApiWithBearerToken.skipIfLocalServerIsNotRunning();
    }

    @Test
    public void testListSchemas_BasicAuth()
    {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost:8080");

        defaultClient.setUsername("test_username");
        defaultClient.setPassword("test_password");

        DefaultApi defaultApi = new DefaultApi(defaultClient);

        List<String> schemas = defaultApi.schemasGet();
        assertEquals(schemas.size(), 3);
        assertTrue(schemas.contains("virtual"));
    }

    @Test
    public void testListSchemas_ApiKey()
    {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost:8080");

        defaultClient.setApiKey("your_hardcoded_api_key");

        DefaultApi defaultApi = new DefaultApi(defaultClient);

        List<String> schemas = defaultApi.schemasGet();
        assertEquals(schemas.size(), 3);
        assertTrue(schemas.contains("virtual"));
    }
}
