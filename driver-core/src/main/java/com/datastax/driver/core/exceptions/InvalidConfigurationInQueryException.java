/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core.exceptions;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * A specific invalid query exception that indicates that the query is invalid
 * because of some configuration problem.
 * <p>
 * This is generally throw by query that manipulate the schema (CREATE and
 * ALTER) when the required configuration options are invalid.
 */
public class InvalidConfigurationInQueryException extends InvalidQueryException implements CoordinatorException {

    private static final long serialVersionUID = 0;

    private final InetSocketAddress address;

    /**
     * @deprecated This constructor is kept for backwards compatibility.
     */
    @Deprecated
    public InvalidConfigurationInQueryException(String msg) {
        this(null, msg);
    }

    public InvalidConfigurationInQueryException(InetSocketAddress address, String msg) {
        super(msg);
        this.address = address;
    }

    /**
     * The coordinator host that caused this exception to be thrown.
     *
     * @return The coordinator host that caused this exception to be thrown, or {@code null} if this exception has been generated driver-side.
     */
    public InetAddress getHost() {
        return address.getAddress();
    }

    /**
     * The full address of the coordinator host that caused this exception to be thrown.
     *
     * @return the full address of the coordinator host that caused this exception to be thrown,
     * or {@code null} if this exception has been generated driver-side.
     */
    public InetSocketAddress getAddress() {
        return address;
    }

}
