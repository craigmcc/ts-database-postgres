// index ---------------------------------------------------------------------

// Return a new instance of our Connection class implementation.

// External Modules ----------------------------------------------------------

import {
    Connection,
    ConnectionURI,
    ConnectionAttributes
} from "@craigmcc/ts-database";

// Internal Modules ----------------------------------------------------------

import ConnectionImpl from "./ConnectionImpl";

export const connection
    = (params: ConnectionURI | ConnectionAttributes )
    : Connection =>
{
    return new ConnectionImpl(params);
}

export default connection;

