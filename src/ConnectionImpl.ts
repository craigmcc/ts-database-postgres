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
    NotConnectedError,
    NotSupportedError,
} from "@craigmcc/ts-database";
const { Client } = require("pg");

// Internal Modules ----------------------------------------------------------

// Public Objects ------------------------------------------------------------

export class ConnectionImpl implements Connection {

    // Constructor
    constructor (params: ConnectionURI | ConnectionAttributes) {
        this.params = params;
    }

    // Private Members -------------------------------------------------------

    private client = new Client();
    private params: ConnectionURI | ConnectionAttributes;

    private checkConnected = (): void => {
        if (!this.connected) {
            throw new NotConnectedError("TsDatabasePostgresql: Not connected");
        }
    }

    // ConnectionOperations Methods ------------------------------------------

    addDatabase
        = async (databaseName: string, options?: object | undefined)
        : Promise<void> =>
    {
        throw new NotSupportedError("addDatabase");
    }

    connect = async (): Promise<void> =>
    {
        if (typeof this.params === "string") {
            this.client = new Client({connectionString: this.params});
        } else {
            this.client = new Client(this.params);
        }
        await this.client.connect();
        this.connected = true;
    }

    connected: boolean = false;

    disconnect
        = async ()
        : Promise<void> =>
    {
        this.checkConnected();
        await this.client.end();
        this.connected = false;
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
        this.checkConnected();
        throw new NotSupportedError("addColumn");
    }

    addForeignKey
        = async (tableName: string, columnName: string, attributes: ForeignKeyAttributes, options?: object | undefined)
        : Promise<string> =>
    {
        this.checkConnected();
        throw new NotSupportedError("addForeignKey");
    }

    addIndex
        = async (tableName: string, attributes: IndexAttributes, options?: object | undefined)
        : Promise<string> =>
    {
        this.checkConnected();
        throw new NotSupportedError("addIndex");
    }

    addTable
        = async (tableName: string, attributes: ColumnAttributes[], options?: object | undefined)
        : Promise<void> =>
    {
        this.checkConnected();
        throw new NotSupportedError("addTable");
    }

    dropColumn
        = async (tableName: string, columnName: string, options?: object | undefined)
        : Promise<void> =>
    {
        this.checkConnected();
        throw new NotSupportedError("dropColumn");
    }

    dropForeignKey
        = async (tableName: string, foreignKey: string, options?: object | undefined)
        : Promise<void> =>
    {
        this.checkConnected();
        throw new NotSupportedError("dropForeignKey");
    }

    dropIndex
        = async (tableName: string, indexName: string, options?: object | undefined)
        : Promise<void> =>
    {
        this.checkConnected();
        throw new NotSupportedError("dropIndex");
    }

    dropRows
        = async (tableName: string, options?: object | undefined)
        : Promise<void> =>
    {
        this.checkConnected();
        throw new NotSupportedError("dropRows");
    }

    dropTable
        = async (tableName: string, options?: object | undefined)
        : Promise<void> =>
    {
        this.checkConnected();
        throw new NotSupportedError("dropTable");
    }

    dropTables
        = async (options?: object | undefined)
        : Promise<void> =>
    {
        this.checkConnected();
        throw new NotSupportedError("dropTables");
    }

    // DmlOperations Methods -------------------------------------------------

    insert
        = async (tableName: string, row: DataObject, options?: object | undefined)
        : Promise<DataObject> =>
    {
        this.checkConnected();
        throw new NotSupportedError("insert");
    };

    inserts
        = async (tableName: string, row: DataObject[], options?: object | undefined)
        : Promise<DataObject[]> =>
    {
        this.checkConnected();
        throw new NotSupportedError("inserts");
    }

}

export default ConnectionImpl;
