/// <reference types="@cloudflare/workers-types" />

import { Client, ResultSet } from '@libsql/client';
import { DefaultLogger, Logger } from '~/logger';
import { BaseSQLiteDatabase } from '~/sqlite-core/db';
import { SQLiteAsyncDialect } from '~/sqlite-core/dialect';
import { LibSQLSession } from './session';

export interface DrizzleConfig {
	logger?: boolean | Logger;
}

export type DrizzleLibSQLDatabase = BaseSQLiteDatabase<'async', ResultSet>;

export function drizzle(client: Client, config: DrizzleConfig = {}): DrizzleLibSQLDatabase {
	const dialect = new SQLiteAsyncDialect();
	let logger;
	if (config.logger === true) {
		logger = new DefaultLogger();
	} else if (config.logger !== false) {
		logger = config.logger;
	}
	const session = new LibSQLSession(client, dialect, { logger });
	return new BaseSQLiteDatabase(dialect, session);
}
