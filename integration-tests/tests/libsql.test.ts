import 'dotenv/config';

import type { TestFn } from 'ava';
import anyTest from 'ava';
import { sql } from 'drizzle-orm';
import type { DrizzleLibSQLDatabase } from 'drizzle-orm/libsql';
import { Client, createClient } from '@libsql/client';
import { drizzle } from 'drizzle-orm/libsql';
import { name } from 'drizzle-orm/sql';
import { integer, blob, sqliteTable, text } from 'drizzle-orm/sqlite-core';

const ENABLE_LOGGING = false;

interface Context {
	client: Client;
	db: DrizzleLibSQLDatabase;
}

const test = anyTest as TestFn<Context>;

const usersTable = sqliteTable('users', {
	id: integer('id').primaryKey(),
	name: text('name').notNull(),
	verified: integer('verified').notNull().default(0),
	json: blob<string[]>('json', { mode: 'json' }),
	createdAt: integer('created_at', { mode: 'timestamp' }).notNull().defaultNow(),
});

const users2Table = sqliteTable('users2', {
	id: integer('id').primaryKey(),
	name: text('name').notNull(),
	cityId: integer('city_id').references(() => citiesTable.id),
});

const citiesTable = sqliteTable('cities', {
	id: integer('id').primaryKey(),
	name: text('name').notNull(),
});

const coursesTable = sqliteTable('courses', {
	id: integer('id').primaryKey(),
	name: text('name').notNull(),
	categoryId: integer('category_id').references(() => courseCategoriesTable.id),
});

const courseCategoriesTable = sqliteTable('course_categories', {
	id: integer('id').primaryKey(),
	name: text('name').notNull(),
});

const orders = sqliteTable('orders', {
	id: integer('id').primaryKey(),
	region: text('region').notNull(),
	product: text('product').notNull(),
	amount: integer('amount').notNull(),
	quantity: integer('quantity').notNull(),
});

const usersMigratorTable = sqliteTable('users12', {
	id: integer('id').primaryKey(),
	name: text('name').notNull(),
	email: text('email').notNull(),
});

const anotherUsersMigratorTable = sqliteTable('another_users', {
	id: integer('id').primaryKey(),
	name: text('name').notNull(),
	email: text('email').notNull(),
});

const pkExample = sqliteTable('pk_example', {
	id: integer('id').primaryKey(),
	name: text('name').notNull(),
	email: text('email').notNull(),
}, (table) => ({
	compositePk: primaryKey(table.id, table.name),
}));


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

	const drops = []
	drops.push(ctx.db.run(sql`drop table if exists ${usersTable}`));
	drops.push(ctx.db.run(sql`drop table if exists ${users2Table}`));
	drops.push(ctx.db.run(sql`drop table if exists ${citiesTable}`));
	drops.push(ctx.db.run(sql`drop table if exists ${coursesTable}`));
	drops.push(ctx.db.run(sql`drop table if exists ${courseCategoriesTable}`));
	drops.push(ctx.db.run(sql`drop table if exists ${orders}`));
	await Promise.all(drops);

	const creates = [];

	creates.push(ctx.db.run(sql`
		create table ${usersTable} (
			id integer primary key,
			name text not null,
			verified integer not null default 0,
			json blob,
			created_at integer not null default (cast((julianday('now') - 2440587.5)*86400000 as integer))
		)`));

	creates.push(ctx.db.run(sql`
		create table ${citiesTable} (
			id integer primary key,
			name text not null
		)`));
	creates.push(ctx.db.run(sql`
			create table ${courseCategoriesTable} (
				id integer primary key,
				name text not null
			)`));

	creates.push(ctx.db.run(sql`
		create table ${users2Table} (
			id integer primary key,
			name text not null,
			city_id integer references ${citiesTable}(${name(citiesTable.id.name)})
		)`));
	creates.push(ctx.db.run(sql`
		create table ${coursesTable} (
			id integer primary key,
			name text not null,
			category_id integer references ${courseCategoriesTable}(${name(courseCategoriesTable.id.name)})
		)`));
	creates.push(ctx.db.run(sql`
		create table ${orders} (
			id integer primary key,
			region text not null,
			product text not null,
			amount integer not null,
			quantity integer not null
		)`));

	await Promise.all(creates);
});

test.serial('select all fields', async (t) => {
	const { db } = t.context;

	const result = await db.run(sql`SELECT * FROM users`);
	t.assert(result.rows?.length == 0);
});
