// connect.test --------------------------------------------------------------

// Basic connection tests for @craigmcc/ts-database-postgres

// External Modules -----------------------------------------------------------

const chai = require("chai");
require("custom-env").env(true);
const expect = chai.expect;
import connection, { InvalidNameError } from "@craigmcc/ts-database";

// Internal Modules -----------------------------------------------------------

import ConnectionImpl from "../ConnectionImpl";

const CONNECTION_URI = process.env.CONNECTION_URI ? process.env.CONNECTION_URI : "UNKNOWN";
const db = new ConnectionImpl(CONNECTION_URI);

// Test Suites ----------------------------------------------------------------

describe("connect", () => {

    it("should successfully connect", async () => {
        try {
            await db.connect();
            expect(db.connected).equals(true);
        } catch (error) {
            expect.fail(`Should not have thrown '${error.message}'`);
        }
    })

    it("should successfully disconnect", async () => {
        try {
            await db.connect();
            expect(db.connected).equals(true);
            await db.disconnect();
            expect(db.connected).equals(false);
        } catch (error) {
            expect.fail(`Should not have thrown '${error.message}'`);
        }
    })

})
