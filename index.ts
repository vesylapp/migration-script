import { config } from "dotenv";
config();

import * as fs from "fs";
import * as z from "zod";
import clerkClient from "@clerk/clerk-sdk-node";
import ora, { Ora } from "ora";
import { Pool } from "pg";

const SECRET_KEY = process.env.CLERK_SECRET_KEY;
const POSTGRES_URL = process.env.POSTGRES_URL;
const DELAY = parseInt(process.env.DELAY_MS ?? `1_000`);
const RETRY_DELAY = parseInt(process.env.RETRY_DELAY_MS ?? `10_000`);
const IMPORT_TO_DEV = process.env.IMPORT_TO_DEV_INSTANCE ?? "false";
const OFFSET = parseInt(process.env.OFFSET ?? `0`);

if (!SECRET_KEY) {
	throw new Error(
		"CLERK_SECRET_KEY is required. Please copy .env.example to .env and add your key."
	);
}

if (!POSTGRES_URL) {
	throw new Error(
		"POSTGRES_URL is required. Please add your PostgreSQL connection URL to .env."
	);
}

if (SECRET_KEY.split("_")[1] !== "live" && IMPORT_TO_DEV === "false") {
	throw new Error(
		"The Clerk Secret Key provided is for a development instance. Development instances are limited to 500 users and do not share their userbase with production instances. If you want to import users to your development instance, please set 'IMPORT_TO_DEV_INSTANCE' in your .env to 'true'."
	);
}

const pool = new Pool({
	connectionString: POSTGRES_URL,
});

interface UserRecord {
	id: number;
	email: string;
	first_name: string;
	last_name: string;
	password: string;
	company: string;
	clerk_organization_id: string | null;
}

interface LoginRecord {
	id: number;
	email: string;
	first_name: string;
	last_name: string;
	password: string;
	seller_id: number;
}

const now = new Date().toISOString().split(".")[0]; // YYYY-MM-DDTHH:mm:ss
function appendLog(payload: any) {
	fs.appendFileSync(
		`./migration-log-${now}.json`,
		`\n${JSON.stringify(payload, null, 2)}`
	);
}

let migrated = 0;
let alreadyExists = 0;

async function createOrganization(user: UserRecord): Promise<string> {
	try {
		const organization = await clerkClient.organizations.createOrganization({
			name: user.company,
			createdBy: user.email,
		});
		return organization.id;
	} catch (error: any) {
		appendLog({ 
			userId: user.id, 
			error: `Failed to create organization: ${error.message}` 
		});
		throw error;
	}
}

async function createClerkUser(login: LoginRecord, organizationId: string) {
	try {
		const user = await clerkClient.users.createUser({
			emailAddress: [login.email],
			firstName: login.first_name,
			lastName: login.last_name,
			passwordDigest: login.password,
			passwordHasher: "bcrypt",
		});

		await clerkClient.organizations.createOrganizationMembership({
			organizationId: organizationId,
			userId: user.id,
			role: "basic_member"
		});

		return user;
	} catch (error: any) {
		if (error?.status === 422) {
			appendLog({ 
				loginId: login.id, 
				error: "User already exists" 
			});
			alreadyExists++;
			return null;
		}
		throw error;
	}
}

async function processUser(user: UserRecord, spinner: Ora) {
	const txt = spinner.text;
	try {
		// Create organization if it doesn't exist
		let organizationId = user.clerk_organization_id;
		if (!organizationId) {
			organizationId = await createOrganization(user);
			// Update the user record with the new organization ID
			await pool.query(
				"UPDATE users SET clerk_organization_id = $1 WHERE id = $2",
				[organizationId, user.id]
			);
		}

		// Get all logins for this user
		const { rows: logins } = await pool.query<LoginRecord>(
			"SELECT * FROM logins WHERE seller_id = $1",
			[user.id]
		);

		// Process each login
		for (const login of logins) {
			spinner.text = `Processing login ${login.id} for user ${user.id}`;
			await createClerkUser(login, organizationId);
			migrated++;
			await new Promise((r) => setTimeout(r, DELAY));
		}
	} catch (error: any) {
		if (error?.status === 429) {
			spinner.text = `${txt} - rate limit reached, waiting for ${RETRY_DELAY} ms`;
			await new Promise((r) => setTimeout(r, RETRY_DELAY));
			spinner.text = txt;
			return processUser(user, spinner);
		}
		appendLog({ 
			userId: user.id, 
			error: error.message 
		});
	}
}

async function main() {
	console.log(`Clerk User Migration Utility`);

	const spinner = ora("Fetching users from database").start();
	
	try {
		const { rows: users } = await pool.query<UserRecord>(
			"SELECT * FROM users ORDER BY id OFFSET $1",
			[OFFSET]
		);

		spinner.text = `Found ${users.length} users to process`;

		for (const user of users) {
			await processUser(user, spinner);
		}

		spinner.succeed(`Migration complete`);
		console.log(`${migrated} logins migrated`);
		console.log(`${alreadyExists} logins already existed`);
	} catch (error) {
		spinner.fail("Migration failed");
		console.error(error);
	} finally {
		await pool.end();
	}
}

main();
