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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

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
        binder.bind(OpenAPIService.class).to(DefaultOpenAPIService.class).in(Scopes.SINGLETON);
        binder.bind(OpenAPIConnector.class).in(Scopes.SINGLETON);
        binder.bind(OpenAPISplitManager.class).in(Scopes.SINGLETON);
        binder.bind(OpenAPIPageSourceProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(OpenAPIConnectorConfig.class);
    }
}
