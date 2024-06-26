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

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.ServiceLoader;

import static com.facebook.airlift.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.assertNotNull;

public class TestOpenAPIPlugin
{
    @Test
    public void testPlugin()
    {
        OpenAPIPlugin plugin = loadPlugin(OpenAPIPlugin.class);

        ConnectorFactory factory = Iterables.getOnlyElement(plugin.getConnectorFactories());
        assertInstanceOf(factory, OpenAPIConnectorFactory.class);

        Map<String, String> configA = ImmutableMap.of("presto-openapi.base_url", "http://localhost:8080");
        Connector firstConnector = factory.create("testA", configA, new TestingConnectorContext());

        assertNotNull(firstConnector);
        assertInstanceOf(firstConnector, OpenAPIConnector.class);

        // Instantiate another connector with different configuration to test multiple connectors

        Map<String, String> configB = ImmutableMap.of("presto-openapi.base_url", "http://localhost:8085");
        Connector secondConnector = factory.create("testB", configB, new TestingConnectorContext());

        assertNotNull(secondConnector);
        assertInstanceOf(secondConnector, OpenAPIConnector.class);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Plugin> T loadPlugin(Class<T> clazz)
    {
        for (Plugin plugin : ServiceLoader.load(Plugin.class)) {
            if (clazz.isInstance(plugin)) {
                return (T) plugin;
            }
        }
        throw new AssertionError("did not find plugin: " + clazz.getName());
    }
}
