import 'dotenv/config';

import type { TestFn } from 'ava';
import anyTest from 'ava';
import { sql } from 'drizzle-orm';
import { asc, eq, gt, inArray } from 'drizzle-orm/expressions';
import type { NodePgDatabase } from 'drizzle-orm/node-postgres';
import { drizzle } from 'drizzle-orm/node-postgres';
import { migrate } from 'drizzle-orm/node-postgres/migrator';
import type { AnyPgColumn, InferModel } from 'drizzle-orm/pg-core';
import {
	alias,
	boolean,
	char,
	cidr,
	inet,
	integer,
	jsonb,
	macaddr,
	macaddr8,
	pgTable,
	serial,
	text,
	timestamp,
} from 'drizzle-orm/pg-core';
import { getMaterializedViewConfig, getViewConfig } from 'drizzle-orm/pg-core/utils';
import { pgMaterializedView, pgView } from 'drizzle-orm/pg-core/view';
import type { SQL, SQLWrapper } from 'drizzle-orm/sql';
import { name, placeholder } from 'drizzle-orm/sql';
import getPort from 'get-port';
import { Client } from 'pg';
import { v4 as uuid } from 'uuid';

const ENABLE_LOGGING = false;

const usersTable = pgTable('users', {
	id: serial('id').primaryKey(),
	name: text('name').notNull(),
	verified: boolean('verified').notNull().default(false),
	jsonb: jsonb<string[]>('jsonb'),
	createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
});

const citiesTable = pgTable('cities', {
	id: serial('id').primaryKey(),
	name: text('name').notNull(),
	state: char('state', { length: 2 }),
});

const users2Table = pgTable('users2', {
	id: serial('id').primaryKey(),
	name: text('name').notNull(),
	cityId: integer('city_id').references(() => citiesTable.id),
});

const coursesTable = pgTable('courses', {
	id: serial('id').primaryKey(),
	name: text('name').notNull(),
	categoryId: integer('category_id').references(() => courseCategoriesTable.id),
});

const courseCategoriesTable = pgTable('course_categories', {
	id: serial('id').primaryKey(),
	name: text('name').notNull(),
});

const orders = pgTable('orders', {
	id: serial('id').primaryKey(),
	region: text('region').notNull(),
	product: text('product').notNull(),
	amount: integer('amount').notNull(),
	quantity: integer('quantity').notNull(),
});

const network = pgTable('network_table', {
	inet: inet('inet').notNull(),
	cidr: cidr('cidr').notNull(),
	macaddr: macaddr('macaddr').notNull(),
	macaddr8: macaddr8('macaddr8').notNull(),
});

const salEmp = pgTable('sal_emp', {
	name: text('name'),
	payByQuarter: integer('pay_by_quarter').array(),
	schedule: text('schedule').array().array(),
});

const tictactoe = pgTable('tictactoe', {
	squares: integer('squares').array(3).array(3),
});

const usersMigratorTable = pgTable('users12', {
	id: serial('id').primaryKey(),
	name: text('name').notNull(),
	email: text('email').notNull(),
});

interface Context {
	client: Client;
	db: DrizzleLibSQLDatabase;
}

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
			ctx.client = new Client(connectionString);
			await ctx.client.connect();
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
		await ctx.client?.end().catch(console.error);
		throw lastError;
	}
	ctx.db = drizzle(ctx.client, { logger: ENABLE_LOGGING });
});

test.after.always(async (t) => {
	const ctx = t.context;
	await ctx.client?.end().catch(console.error);
});

test.beforeEach(async (t) => {
	const ctx = t.context;
	await ctx.db.execute(sql`drop schema public cascade`);
	await ctx.db.execute(sql`create schema public`);
	await ctx.db.execute(
		sql`create table users (
			id serial primary key,
			name text not null,
			verified boolean not null default false, 
			jsonb jsonb,
			created_at timestamptz not null default now()
		)`,
	);
	await ctx.db.execute(
		sql`create table cities (
			id serial primary key,
			name text not null,
			state char(2)
		)`,
	);
	await ctx.db.execute(
		sql`create table users2 (
			id serial primary key,
			name text not null,
			city_id integer references cities(id)
		)`,
	);
	await ctx.db.execute(
		sql`create table course_categories (
			id serial primary key,
			name text not null
		)`,
	);
	await ctx.db.execute(
		sql`create table courses (
			id serial primary key,
			name text not null,
			category_id integer references course_categories(id)
		)`,
	);
	await ctx.db.execute(
		sql`create table orders (
			id serial primary key,
			region text not null,
			product text not null,
			amount integer not null,
			quantity integer not null
		)`,
	);
	await ctx.db.execute(
		sql`create table network_table (
			inet inet not null,
			cidr cidr not null,
			macaddr macaddr not null,
			macaddr8 macaddr8 not null
		)`,
	);
	await ctx.db.execute(
		sql`create table sal_emp (
			name text not null,
			pay_by_quarter integer[] not null,
			schedule text[][] not null
		)`,
	);
	await ctx.db.execute(
		sql`create table tictactoe (
			squares integer[3][3] not null
		)`,
	);
});

test.serial('select all fields', async (t) => {
	const { db } = t.context;

	const now = Date.now();

	await db.insert(usersTable).values({ name: 'John' });
	const result = await db.select().from(usersTable);

	t.assert(result[0]!.createdAt instanceof Date);
	t.assert(Math.abs(result[0]!.createdAt.getTime() - now) < 100);
	t.deepEqual(result, [
		{ id: 1, name: 'John', verified: false, jsonb: null, createdAt: result[0]!.createdAt },
	]);
});
