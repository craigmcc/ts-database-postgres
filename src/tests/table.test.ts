// table.test ----------------------------------------------------------------

// Table management tests for @craigmcc/ts-database-postgres

// External Modules ----------------------------------------------------------

const chai = require("chai");
require("custom-env").env(true);
const expect = chai.expect;
import {ColumnAttributes, DataType, TableNotFoundError,} from "@craigmcc/ts-database";

// Internal Modules ----------------------------------------------------------
import ConnectionImpl from "../ConnectionImpl";

const CONNECTION_URI = process.env.CONNECTION_URI ? process.env.CONNECTION_URI : "UNKNOWN";
const db = new ConnectionImpl(CONNECTION_URI);

const TEST_COLUMNS: ColumnAttributes[] = [
    { name: "id", primaryKey: true, type: DataType.INTEGER, allowNull: false },
    { name: "first_name", type: DataType.STRING, allowNull: false },
    { name: "last_name", type: DataType.STRING, allowNull: false },
    { name: "active", type: DataType.BOOLEAN, allowNull: false, defaultValue: "true" },
    { name: "age", type: DataType.SMALLINT, allowNull: true },
    { name: "created", type: DataType.DATETIME, allowNull: true },
    { name: "comments", type: DataType.STRING, allowNull: true },
    { name: "started_date", type: DataType.DATE, allowNull: true },
    { name: "started_time", type: DataType.TIME, allowNull: true },
];
const TEST_TABLE = "test_table";

// Test Lifecycle Hooks ------------------------------------------------------

beforeEach("beforeEach", async () => {
    try {
        await db.connect();
        expect(db.connected).equals(true);
    } catch (error) {
        expect.fail(`beforeEach: Should not have thrown '${error.message}'`);
    }
    try {
        await db.dropTable(TEST_TABLE);
    } catch (error) {
        // Ignore any errors from dropping the table that might not be there
    }
})

afterEach("afterEach", async () => {
    try {
        await db.dropTable(TEST_TABLE);
    } catch (error) {
        // Ignore any errors from dropping the table that might not be there
    }
    try {
        await db.disconnect();
        expect(db.connected).equals(false);
    } catch (error) {
        expect.fail(`afterEach: Should not have thrown '${error.message}'`);
    }
})

// Test Suites ---------------------------------------------------------------

describe ("addDescribeDropTable", () => {

    it("should fail on duplicate add", async () => {
        try {
            await db.addTable(TEST_TABLE, TEST_COLUMNS);
        } catch (error) {
            expect.fail(`ADD: Should not have thrown '${error.message}'`);
        }
        try {
            await db.addTable(TEST_TABLE, TEST_COLUMNS);
            expect.fail("Should have thrown error on duplicate table");
        } catch (error) {
            // Expected result
        }

    })

    it("should pass on valid parameters", async () => {
        try {
            await db.addTable(TEST_TABLE, TEST_COLUMNS);
        } catch (error) {
            expect.fail(`ADD: Should not have thrown '${error.message}'`);
        }
        try {
            const results = await db.describeTable(TEST_TABLE);
        } catch (error) {
            expect.fail(`DESC: Should not have thrown '${error.message}'`);
        }
        try {
            await db.dropTable(TEST_TABLE);
        } catch (error) {
            expect.fail(`DROP: Should not have thrown '${error.message}'`);
        }
        try {
            await db.describeTable(TEST_TABLE);
            expect.fail("Should have thrown error for missing table");
        } catch (error) {
            if (error instanceof TableNotFoundError) {
                expect(error.message).includes(TEST_TABLE);
            } else {
                expect.fail(`DESC2: Should not have thrown '${error.message}'`);
            }
        }
    })

})

describe("describeTable", () => {

    it("should fail on an invalid table name", async () => {
        const tableName = "nonexistent_table_name";
        try {
            const results = await db.describeTable(tableName);
            expect.fail("Should have thrown TableNotFoundError");
        } catch (error) {
            if (error instanceof TableNotFoundError) {
                expect(error.message).includes(tableName);
            } else {
                expect.fail("Should not have thrown", error);
            }
        }
    })

})

describe("dropTable", () => {

    it("should fail on invalid table name", async () => {
        const tableName = "nonexistent_table_name";
        try {
            await db.dropTable(tableName);
            expect.fail("Should have thrown TableNotFoundError");
        } catch (error) {
            if (error instanceof TableNotFoundError) {
                expect(error.message).includes(tableName);
            } else {
                expect.fail("Should not have thrown", error);
            }
        }
    })

})
