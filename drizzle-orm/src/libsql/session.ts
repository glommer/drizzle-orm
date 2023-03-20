/// <reference types="@cloudflare/workers-types" />

import type { Client, ResultSet } from '@libsql/client';
import { Logger, NoopLogger } from '~/logger';
import type { fillPlaceholders, Query } from '~/sql';
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

	run(placeholderValues?: Record<string, unknown>): Promise<ResultSet> {
		console.log(this.queryString);
		// TODO: params
		return this.client.execute(this.queryString);
	}

	all(placeholderValues?: Record<string, unknown>): Promise<T['all']> {
		// TODO
		throw new Error('Not implemented');
	}

	get(placeholderValues?: Record<string, unknown>): Promise<T['get']> {
		// TODO
		throw new Error('Not implemented');
	}

	values<T extends any[] = unknown[]>(placeholderValues?: Record<string, unknown>): Promise<T[]> {
		// TODO
		throw new Error('Not implemented');
	}
}
