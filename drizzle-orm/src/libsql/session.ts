/// <reference types="@cloudflare/workers-types" />

import type { Client, ResultSet, SqlValue } from '@libsql/client';
import { Logger, NoopLogger } from '~/logger';
import { fillPlaceholders, Query } from '~/sql';
import type { SQLiteAsyncDialect } from '~/sqlite-core/dialect';
import type { SelectFieldsOrdered } from '~/sqlite-core/query-builders/select.types';
import {
	PreparedQuery as PreparedQueryBase,
	PreparedQueryConfig as PreparedQueryConfigBase,
	SQLiteSession,
} from '~/sqlite-core/session';
import { mapResultRow } from '~/utils';

export interface LibSQLSessionOptions {
	logger?: Logger;
}

type PreparedQueryConfig = Omit<PreparedQueryConfigBase, 'statement' | 'run'>;

export class LibSQLSession extends SQLiteSession<'async', ResultSet> {
	private logger: Logger;

	constructor(
		private client: Client,
		dialect: SQLiteAsyncDialect,
		options: LibSQLSessionOptions = {},
	) {
		super(dialect);
		this.logger = options.logger ?? new NoopLogger();
	}

	exec(query: string): void {
		throw Error('TODO');
	}

	prepareQuery(query: Query, fields?: SelectFieldsOrdered): PreparedQuery {
		return new PreparedQuery(this.client, query.sql, query.params, this.logger, fields);
	}
}

export class PreparedQuery<T extends PreparedQueryConfig = PreparedQueryConfig> extends PreparedQueryBase<
	{ type: 'async'; run: ResultSet; all: T['all']; get: T['get']; values: T['values'] }
> {
	constructor(
		private client: Client,
		private queryString: string,
		private params: unknown[],
		private logger: Logger,
		private fields: SelectFieldsOrdered | undefined,
	) {
		super();
	}

	async run(placeholderValues?: Record<string, unknown>): Promise<ResultSet> {
		const params = fillPlaceholders(this.params, placeholderValues ?? {});
		return await this.client.execute(this.queryString, params as SqlValue[]);
	}

	async all(placeholderValues?: Record<string, unknown>): Promise<T['all']> {
		const params = fillPlaceholders(this.params, placeholderValues ?? {});
		const results = await this.client.execute(this.queryString, params as SqlValue[]);
		return results.rows as unknown[];
	}

	get(placeholderValues?: Record<string, unknown>): Promise<T['get']> {
		// TODO
		throw new Error('get() not implemented');
	}

	values<T extends any[] = unknown[]>(placeholderValues?: Record<string, unknown>): Promise<T[]> {
		// TODO
		throw new Error('values() not implemented');
	}
}
