// ConnectionImpl ------------------------------------------------------------

// Implementation of Connection from ts-database for PostgreSQL.

// External Modules ----------------------------------------------------------

import {
    ColumnAttributes,
    Connection,
    ConnectionAttributes,
    ConnectionURI,
    DataObject,
    ForeignKeyAttributes,
    IndexAttributes,
    NotSupportedError,
} from "@craigmcc/ts-database";
const { Client } = require("pg");

// Internal Modules ----------------------------------------------------------

// Public Objects ------------------------------------------------------------

export class ConnectionImpl implements Connection {

    // Private Variables -----------------------------------------------------

    private client = new Client();

    // ConnectionOperations Methods ------------------------------------------

    addDatabase
        = async (databaseName: string, options?: object | undefined)
        : Promise<void> => {
        throw new NotSupportedError("addDatabase");
    }

    connect
        = async (params: ConnectionURI | ConnectionAttributes)
        : Promise<void> =>
    {
        throw new NotSupportedError("connect");
    }

    connected: boolean = false;

    disconnect
        = async ()
        : Promise<void> =>
    {
        throw new NotSupportedError("disconnect");
    }

    dropDatabase
        = async (databaseName: string, options?: object | undefined)
        : Promise<void> =>
    {
        throw new NotSupportedError("dropDatabase");
    }

    // DdlOperations Methods -------------------------------------------------

    addColumn
        = async (tableName: string, attributes: ColumnAttributes | ColumnAttributes[], options?: object | undefined)
        : Promise<void> =>
    {
        throw new NotSupportedError("addColumn");
    }

    addForeignKey
        = async (tableName: string, columnName: string, attributes: ForeignKeyAttributes, options?: object | undefined)
        : Promise<string> =>
    {
        throw new NotSupportedError("addForeignKey");
    }

    addIndex
        = async (tableName: string, attributes: IndexAttributes, options?: object | undefined)
        : Promise<string> =>
    {
        throw new NotSupportedError("addIndex");
    }

    addTable
        = async (tableName: string, attributes: ColumnAttributes[], options?: object | undefined)
        : Promise<void> =>
    {
        throw new NotSupportedError("addTable");
    }

    dropColumn
        = async (tableName: string, columnName: string, options?: object | undefined)
        : Promise<void> =>
    {
        throw new NotSupportedError("dropColumn");
    }

    dropForeignKey
        = async (tableName: string, foreignKey: string, options?: object | undefined)
        : Promise<void> =>
    {
        throw new NotSupportedError("dropForeignKey");
    }

    dropIndex
        = async (tableName: string, indexName: string, options?: object | undefined)
        : Promise<void> =>
    {
        throw new NotSupportedError("dropIndex");
    }

    dropRows
        = async (tableName: string, options?: object | undefined)
        : Promise<void> =>
    {
        throw new NotSupportedError("dropRows");
    }

    dropTable
        = async (tableName: string, options?: object | undefined)
        : Promise<void> =>
    {
        throw new NotSupportedError("dropTable");
    }

    dropTables
        = async (options?: object | undefined)
        : Promise<void> =>
    {
        throw new NotSupportedError("dropTables");
    }

    // DmlOperations Methods -------------------------------------------------

    insert
        = async (tableName: string, row: DataObject, options?: object | undefined)
        : Promise<DataObject> =>
    {
        throw new NotSupportedError("insert");
    };

    inserts
        = async (tableName: string, row: DataObject[], options?: object | undefined)
        : Promise<DataObject> =>
    {
        throw new NotSupportedError("inserts");
    }

}

export default ConnectionImpl;
