import type { PgDialect } from '~/pg-core/dialect';
import type { IndexColumn } from '~/pg-core/indexes';
import type { PgSession, PreparedQuery, PreparedQueryConfig, QueryResultHKT, QueryResultKind } from '~/pg-core/session';
import type { AnyPgTable, InferModel} from '~/pg-core/table';
import { PgTable } from '~/pg-core/table';
import { QueryPromise } from '~/query-promise';
import type { Placeholder, Query, SQLWrapper } from '~/sql';
import { Param, SQL, sql } from '~/sql';
import { Table } from '~/table';
import type { Simplify } from '~/utils';
import { mapUpdateSet, orderSelectedFields } from '~/utils';
import type { SelectFieldsFlat, SelectFieldsOrdered, SelectResultFields } from './select.types';
import type { PgUpdateSetSource } from './update';

export interface PgInsertConfig<TTable extends AnyPgTable = AnyPgTable> {
	table: TTable;
	values: Record<string, Param | SQL>[];
	onConflict?: SQL;
	returning?: SelectFieldsOrdered;
}

export type PgInsertValue<TTable extends AnyPgTable> = {
	[Key in keyof InferModel<TTable, 'insert'>]: InferModel<TTable, 'insert'>[Key] | SQL | Placeholder;
};

export class PgInsertBuilder<TTable extends AnyPgTable, TQueryResult extends QueryResultHKT> {
	constructor(
		private table: TTable,
		private session: PgSession,
		private dialect: PgDialect,
	) {}

	values(...values: PgInsertValue<TTable>[]): PgInsert<TTable, TQueryResult> {
		const mappedValues = values.map((entry) => {
			const result: Record<string, Param | SQL> = {};
			const cols = this.table[Table.Symbol.Columns];
			for (const colKey of Object.keys(entry)) {
				const colValue = entry[colKey as keyof typeof entry];
				if (colValue instanceof SQL) {
					result[colKey] = colValue;
				} else {
					result[colKey] = new Param(colValue, cols[colKey]);
				}
			}
			return result;
		});

		return new PgInsert(this.table, mappedValues, this.session, this.dialect);
	}
}

export interface PgInsert<
	TTable extends AnyPgTable,
	TQueryResult extends QueryResultHKT,
	TReturning extends Record<string, unknown> | undefined = undefined,
> extends
	QueryPromise<TReturning extends undefined ? QueryResultKind<TQueryResult, never> : TReturning[]>,
	SQLWrapper
{}

export class PgInsert<
	TTable extends AnyPgTable,
	TQueryResult extends QueryResultHKT,
	TReturning extends Record<string, unknown> | undefined = undefined,
> extends QueryPromise<TReturning extends undefined ? QueryResultKind<TQueryResult, never> : TReturning[]>
	implements SQLWrapper
{
	declare protected $table: TTable;
	declare protected $return: TReturning;

	private config: PgInsertConfig<TTable>;

	constructor(
		table: TTable,
		values: PgInsertConfig['values'],
		private session: PgSession,
		private dialect: PgDialect,
	) {
		super();
		this.config = { table, values };
	}

	returning(): Omit<PgInsert<TTable, TQueryResult, InferModel<TTable>>, 'returning' | `onConflict${string}`>;
	returning<TSelectedFields extends SelectFieldsFlat>(fields: TSelectedFields): Omit<
		PgInsert<TTable, TQueryResult, SelectResultFields<TSelectedFields>>,
		'returning' | `onConflict${string}`
	>;
	returning(
		fields: SelectFieldsFlat = this.config.table[PgTable.Symbol.Columns],
	): Omit<PgInsert<TTable, any>, 'returning' | `onConflict${string}`> {
		this.config.returning = orderSelectedFields(fields);
		return this;
	}

	onConflictDoNothing(config: { target?: IndexColumn | IndexColumn[]; where?: SQL } = {}): this {
		if (config.target === undefined) {
			this.config.onConflict = sql`do nothing`;
		} else {
			let targetColumn = '';
			if (Array.isArray(config.target)) {
				targetColumn = config.target.map((it) => this.dialect.escapeName(it.name)).join(',');
			} else {
				targetColumn = this.dialect.escapeName(config.target.name);
			}

			const whereSql = config.where ? sql` where ${config.where}` : sql``;
			this.config.onConflict = sql`(${sql.raw(targetColumn)})${whereSql} do nothing`;
		}
		return this;
	}

	onConflictDoUpdate(config: {
		target: IndexColumn | IndexColumn[];
		where?: SQL;
		set: PgUpdateSetSource<TTable>;
	}): this {
		const whereSql = config.where ? sql` where ${config.where}` : sql``;
		const setSql = this.dialect.buildUpdateSet(this.config.table, mapUpdateSet(this.config.table, config.set));
		let targetColumn = '';
		if (Array.isArray(config.target)) {
			targetColumn = config.target.map((it) => this.dialect.escapeName(it.name)).join(',');
		} else {
			targetColumn = this.dialect.escapeName(config.target.name);
		}
		this.config.onConflict = sql`(${sql.raw(targetColumn)})${whereSql} do update set ${setSql}`;
		return this;
	}

	/** @internal */
	getSQL(): SQL {
		return this.dialect.buildInsertQuery(this.config);
	}

	toSQL(): Simplify<Omit<Query, 'typings'>> {
		const { typings, ...rest } = this.dialect.sqlToQuery(this.getSQL());
		return rest;
	}

	private _prepare(name?: string): PreparedQuery<
		PreparedQueryConfig & {
			execute: TReturning extends undefined ? QueryResultKind<TQueryResult, never> : TReturning[];
		}
	> {
		return this.session.prepareQuery(this.dialect.sqlToQuery(this.getSQL()), this.config.returning, name);
	}

	prepare(name: string): PreparedQuery<
		PreparedQueryConfig & {
			execute: TReturning extends undefined ? QueryResultKind<TQueryResult, never> : TReturning[];
		}
	> {
		return this._prepare(name);
	}

	override execute: ReturnType<this['prepare']>['execute'] = (placeholderValues) => {
		return this._prepare().execute(placeholderValues);
	};
}
