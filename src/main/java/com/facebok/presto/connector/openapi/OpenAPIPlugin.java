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
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Module;

import java.util.List;
import java.util.ServiceLoader;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class OpenAPIPlugin
        implements Plugin
{
    private final String name;
    private final Module module;

    public OpenAPIPlugin()
    {
        this(getPluginConfig());
    }

    public OpenAPIPlugin(OpenAPIPluginConfig pluginConfig)
    {
        this(pluginConfig.getName(), pluginConfig.getModule());
    }

    public OpenAPIPlugin(String name, Module module)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.module = requireNonNull(module);
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new OpenAPIConnectorFactory(name, module));
    }

    private static OpenAPIPluginConfig getPluginConfig()
    {
        ClassLoader classLoader = OpenAPIPlugin.class.getClassLoader();
        ServiceLoader<OpenAPIPluginConfig> loader = ServiceLoader.load(OpenAPIPluginConfig.class, classLoader);
        List<OpenAPIPluginConfig> list = ImmutableList.copyOf(loader);
        return list.isEmpty() ? new OpenAPIPluginConfig() : Iterables.getOnlyElement(list);
    }
}
