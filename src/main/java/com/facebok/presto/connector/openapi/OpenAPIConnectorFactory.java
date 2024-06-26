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

import com.facebok.presto.connector.openapi.util.RebindSafeMBeanServer;
import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.base.Throwables;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.weakref.jmx.guice.MBeanModule;

import javax.management.MBeanServer;

import java.util.Map;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.Objects.requireNonNull;

public class OpenAPIConnectorFactory
        implements ConnectorFactory
{
    private final String name;
    private final Module locationModule;

    public OpenAPIConnectorFactory(String name, Module module)
    {
        this.name = requireNonNull(name);
        this.locationModule = requireNonNull(module);
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new OpenAPIHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        try {
            Bootstrap app = new Bootstrap(
                    new MBeanModule(),
                    binder -> {
                        binder.bind(MBeanServer.class).toInstance(
                                new RebindSafeMBeanServer(getPlatformMBeanServer()));
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                    },
                    locationModule,
                    new OpenAPIModule(catalogName));

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(OpenAPIConnector.class);
        }
        catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
