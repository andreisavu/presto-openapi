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

import com.facebok.presto.connector.openapi.annotations.ConnectorId;
import com.facebok.presto.connector.openapi.annotations.ForMetadataRefresh;
import com.facebook.airlift.concurrent.Threads;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class OpenAPIModule
        implements Module
{
    private final String connectorId;

    public OpenAPIModule(String connectorId)
    {
        this.connectorId = requireNonNull(connectorId);
    }

    @Override
    public void configure(Binder binder)
    {
        // Bind the configs
        binder.bind(String.class).annotatedWith(ConnectorId.class).toInstance(connectorId);
        configBinder(binder).bindConfig(OpenAPIConnectorConfig.class);

        // Bind the services
        binder.bind(OpenAPIService.class).to(DefaultOpenAPIService.class).in(Scopes.SINGLETON);
        binder.bind(OpenAPIConnector.class).in(Scopes.SINGLETON);
        binder.bind(OpenAPIMetadata.class).in(Scopes.SINGLETON);
        binder.bind(OpenAPISplitManager.class).in(Scopes.SINGLETON);
        binder.bind(OpenAPIPageSourceProvider.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForMetadataRefresh
    public Executor createMetadataRefreshExecutor(OpenAPIConnectorConfig config)
    {
        return Executors.newFixedThreadPool(config.getMetadataRefreshThreads(),
                Threads.daemonThreadsNamed("metadata-refresh-%s"));
    }
}
