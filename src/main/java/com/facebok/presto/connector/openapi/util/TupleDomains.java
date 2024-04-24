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

package com.facebok.presto.connector.openapi.util;

import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TimeZoneKey;

import java.util.Locale;

public class TupleDomains
{
    private static final SqlFunctionProperties DEFAULT_DEBUG_PROPERTIES =
            SqlFunctionProperties.builder()
                    .setParseDecimalLiteralAsDouble(false)
                    .setLegacyRowFieldOrdinalAccessEnabled(false)
                    .setTimeZoneKey(TimeZoneKey.UTC_KEY)
                    .setLegacyTimestamp(false)
                    .setLegacyMapSubscript(false)
                    .setSessionStartTime(0)
                    .setSessionLocale(Locale.US)
                    .setSessionUser("toStringDetailed()")
                    .setFieldNamesInJsonCastEnabled(false)
                    .build();

    private TupleDomains() {}

    /**
     * Return a detailed string representation of the TupleDomain that
     * may include the actual values of the domains. The values are potentially
     * sensitive and should not be exposed to the end user.
     */
    public static String toStringDetailed(TupleDomain<?> tupleDomain)
    {
        return tupleDomain.toString(DEFAULT_DEBUG_PROPERTIES);
    }

    public static String toStringDetailed(Range range)
    {
        return range.toString(DEFAULT_DEBUG_PROPERTIES);
    }
}
