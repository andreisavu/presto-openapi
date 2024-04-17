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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.net.URI;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class OpenAPIConnectorSplit
        implements ConnectorSplit
{
    private final String schemaName;
    private final String tableName;
    private final String split;
    private final URI nodeUri;
    private final List<HostAddress> addresses;

    @JsonCreator
    public OpenAPIConnectorSplit(@JsonProperty("schemaName") String schemaName,
                                 @JsonProperty("tableName") String tableName,
                                 @JsonProperty("split") String split,
                                 @JsonProperty("nodeUri") URI nodeUri)
    {
        this.schemaName = requireNonNull(schemaName);
        this.tableName = requireNonNull(tableName);
        this.split = requireNonNull(split);
        this.nodeUri = requireNonNull(nodeUri);
        this.addresses = ImmutableList.of(HostAddress.fromUri(nodeUri));
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public String getSplit()
    {
        return split;
    }

    @JsonProperty
    public URI getNodeUri()
    {
        return nodeUri;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NodeSelectionStrategy.NO_PREFERENCE;
    }

    @Override
    public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OpenAPIConnectorSplit that = (OpenAPIConnectorSplit) o;
        return Objects.equals(schemaName, that.schemaName)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(split, that.split)
                && Objects.equals(nodeUri, that.nodeUri)
                && Objects.equals(addresses, that.addresses);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, split, nodeUri, addresses);
    }

    @Override
    public String toString()
    {
        return "OpenAPIConnectorSplit{" +
                "schemaName='" + schemaName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", split='" + split + '\'' +
                ", nodeUri=" + nodeUri +
                ", addresses=" + addresses +
                '}';
    }
}
