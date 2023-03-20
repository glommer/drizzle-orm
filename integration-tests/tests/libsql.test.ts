import 'dotenv/config';

import type { TestFn } from 'ava';
import anyTest from 'ava';
import { sql } from 'drizzle-orm';
import type { DrizzleLibSQLDatabase } from 'drizzle-orm/libsql';
import { Client, createClient } from '@libsql/client';
import { drizzle } from 'drizzle-orm/libsql';
import { alias, blob, integer, primaryKey, sqliteTable, text } from 'drizzle-orm/sqlite-core';

const ENABLE_LOGGING = false;

interface Context {
	client: Client;
	db: DrizzleLibSQLDatabase;
}

const usersTable = sqliteTable('users', {
	id: integer('id').primaryKey(),
	name: text('name').notNull(),
	verified: integer('verified').notNull().default(0),
	json: blob<string[]>('json', { mode: 'json' }),
	createdAt: integer('created_at', { mode: 'timestamp' }).notNull().defaultNow(),
});

const test = anyTest as TestFn<Context>;

test.before(async (t) => {
	const ctx = t.context;
	const connectionString = process.env['LIBSQL_CONNECTION_STRING'];
	if (!connectionString) {
		throw new Error('LIBSQL_CONNECTION_STRING is not set');
	}
	let sleep = 250;
	let timeLeft = 5000;
	let connected = false;
	let lastError: unknown | undefined;
	do {
		try {
			const config = {
				url: connectionString,
			};
			ctx.client = createClient(config);
			connected = true;
			break;
		} catch (e) {
			lastError = e;
			await new Promise((resolve) => setTimeout(resolve, sleep));
			timeLeft -= sleep;
		}
	} while (timeLeft > 0);
	if (!connected) {
		console.error('Cannot connect to libsql');
		throw lastError;
	}
	ctx.db = drizzle(ctx.client, { logger: ENABLE_LOGGING });
});

test.after.always(async (t) => {
	const ctx = t.context;
});

test.beforeEach(async (t) => {
	const ctx = t.context;
	await ctx.db.run(sql`drop table if exists ${usersTable}`);
	await ctx.db.run(
		sql`create table ${usersTable} (
			id int primary key,
			name text not null
		)`,
	);
});

test.serial('select all fields', async (t) => {
	const { db } = t.context;

	const result = await db.run(sql`SELECT * FROM users`);
	t.assert(result.rows?.length == 0);
});
