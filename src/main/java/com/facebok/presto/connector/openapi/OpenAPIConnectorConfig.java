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
import com.facebook.airlift.configuration.Config;

import javax.validation.constraints.Min;

public class OpenAPIConnectorConfig
{
    String connectorId;
    private String baseUrl;
    private int metadataRefreshThreads = 1;

    private int httpClientConnectTimeoutMs = 10_000;
    private int httpClientReadTimeoutMs = 10_000;
    private int httpClientWriteTimeoutMs = 10_000;

    public String getBaseUrl()
    {
        return baseUrl;
    }

    @ConnectorId
    public OpenAPIConnectorConfig setConnectorId(String connectorId)
    {
        this.connectorId = connectorId;
        return this;
    }

    @Config("presto-openapi.base_url")
    public OpenAPIConnectorConfig setBaseUrl(String baseUrl)
    {
        this.baseUrl = baseUrl;
        return this;
    }

    @Config("presto-openapi.metadata_refresh_threads")
    public OpenAPIConnectorConfig setMetadataRefreshThreads(int metadataRefreshThreads)
    {
        this.metadataRefreshThreads = metadataRefreshThreads;
        return this;
    }

    @Min(1)
    public int getMetadataRefreshThreads()
    {
        return metadataRefreshThreads;
    }

    @Config("presto-openapi.http-client.connect_timeout_ms")
    public void setHttpClientConnectTimeoutMs(int httpClientConnectTimeoutMs)
    {
        this.httpClientConnectTimeoutMs = httpClientConnectTimeoutMs;
    }

    public int getHttpClientConnectTimeoutMs()
    {
        return httpClientConnectTimeoutMs;
    }

    @Config("presto-openapi.http-client.read_timeout_ms")
    public void setHttpClientReadTimeoutMs(int httpClientReadTimeoutMs)
    {
        this.httpClientReadTimeoutMs = httpClientReadTimeoutMs;
    }

    public int getHttpClientReadTimeoutMs()
    {
        return httpClientReadTimeoutMs;
    }

    @Config("presto-openapi.http-client.write_timeout_ms")
    public void setHttpClientWriteTimeoutMs(int httpClientWriteTimeoutMs)
    {
        this.httpClientWriteTimeoutMs = httpClientWriteTimeoutMs;
    }

    public int getHttpClientWriteTimeoutMs()
    {
        return httpClientWriteTimeoutMs;
    }
}
