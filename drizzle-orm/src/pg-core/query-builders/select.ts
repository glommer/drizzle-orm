import type { AnyPgColumn } from '~/pg-core/columns';
import type { PgDialect } from '~/pg-core/dialect';
import type { PgSession, PreparedQuery, PreparedQueryConfig } from '~/pg-core/session';
import type { SubqueryWithSelection } from '~/pg-core/subquery';
import type { AnyPgTable } from '~/pg-core/table';
import { getTableColumns } from '~/pg-core/utils';
import { PgViewBase } from '~/pg-core/view';
import { QueryPromise } from '~/query-promise';
import type { Query, SQL } from '~/sql';
import { SelectionProxyHandler, Subquery, SubqueryConfig } from '~/subquery';
import { Table } from '~/table';
import { applyMixins, type Simplify, type ValueOrArray } from '~/utils';
import { orderSelectedFields } from '~/utils';
import { ViewBaseConfig } from '~/view';
import { QueryBuilder } from './query-builder';
import type {
	BuildSubquerySelection,
	GetSelectTableName,
	GetSelectTableSelection,
	JoinFn,
	JoinNullability,
	JoinType,
	LockConfig,
	LockStrength,
	PgSelectConfig,
	PgSelectHKT,
	PgSelectHKTBase,
	PgSelectQueryBuilderHKT,
	SelectFields,
	SelectMode,
	SelectResult,
} from './select.types';

type CreatePgSelectFromBuilderMode<
	TBuilderMode extends 'db' | 'qb',
	TTableName extends string,
	TSelection,
	TSelectMode extends SelectMode,
> = TBuilderMode extends 'db' ? PgSelect<TTableName, TSelection, TSelectMode>
	: PgSelectQueryBuilder<PgSelectQueryBuilderHKT, TTableName, TSelection, TSelectMode>;

export class PgSelectBuilder<TSelection extends SelectFields | undefined, TBuilderMode extends 'db' | 'qb' = 'db'> {
	constructor(
		private fields: TSelection,
		private session: PgSession | undefined,
		private dialect: PgDialect,
		private withList: Subquery[] = [],
	) {}

	from<TFrom extends AnyPgTable | Subquery | PgViewBase>(
		source: TFrom,
	): CreatePgSelectFromBuilderMode<
		TBuilderMode,
		GetSelectTableName<TFrom>,
		TSelection extends undefined ? GetSelectTableSelection<TFrom> : TSelection,
		TSelection extends undefined ? 'single' : 'partial'
	> {
		const isPartialSelect = !!this.fields;

		let fields: SelectFields;
		if (this.fields) {
			fields = this.fields;
		} else if (source instanceof Subquery) {
			// This is required to use the proxy handler to get the correct field values from the subquery
			fields = Object.fromEntries(
				Object.keys(source[SubqueryConfig].selection).map((
					key,
				) => [key, source[key as unknown as keyof typeof source] as unknown as SelectFields[string]]),
			);
		} else if (source instanceof PgViewBase) {
			fields = source[ViewBaseConfig].selection as SelectFields;
		} else {
			fields = getTableColumns(source);
		}

		const fieldsList = orderSelectedFields<AnyPgColumn>(fields);
		return new PgSelect(source, fields, fieldsList, isPartialSelect, this.session, this.dialect, this.withList) as any;
	}
}

export abstract class PgSelectQueryBuilder<
	THKT extends PgSelectHKTBase,
	TTableName extends string,
	TSelection,
	TSelectMode extends SelectMode,
	TNullabilityMap extends Record<string, JoinNullability> = Record<TTableName, 'not-null'>,
> extends QueryBuilder<BuildSubquerySelection<TSelection, TNullabilityMap>> {
	declare protected $selectMode: TSelectMode;
	declare protected $selection: TSelection;
	declare protected $subquerySelection: BuildSubquerySelection<TSelection, TNullabilityMap>;

	protected config: PgSelectConfig;
	protected joinsNotNullableMap: Record<string, boolean>;
	private tableName: string;

	constructor(
		table: PgSelectConfig['table'],
		fields: PgSelectConfig['fields'],
		fieldsList: PgSelectConfig['fieldsList'],
		private isPartialSelect: boolean,
		protected session: PgSession | undefined,
		protected dialect: PgDialect,
		withList: Subquery[],
	) {
		super();
		this.config = {
			withList,
			table,
			fields,
			fieldsList,
			joins: {},
			orderBy: [],
			groupBy: [],
			lockingClauses: [],
		};
		this.$subquerySelection = fields as BuildSubquerySelection<TSelection, TNullabilityMap>;
		this.tableName = table instanceof Subquery
			? table[SubqueryConfig].alias
			: table instanceof PgViewBase
			? table[ViewBaseConfig].name
			: table[Table.Symbol.Name];
		this.joinsNotNullableMap = { [this.tableName]: true };
	}

	private createJoin<TJoinType extends JoinType>(
		joinType: TJoinType,
	): JoinFn<THKT, TTableName, TSelectMode, TJoinType, TSelection, TNullabilityMap> {
		return (
			table: AnyPgTable | Subquery,
			on: ((aliases: TSelection) => SQL | undefined) | SQL | undefined,
		) => {
			const tableName = table instanceof Subquery ? table[SubqueryConfig].alias : table[Table.Symbol.Name];

			if (this.config.joins[tableName]) {
				throw new Error(`Alias "${tableName}" is already used in this query`);
			}

			if (!this.isPartialSelect) {
				// If this is the first join and this is not a partial select, "move" the fields from the main table to the nested object
				if (Object.keys(this.joinsNotNullableMap).length === 1) {
					this.config.fieldsList = this.config.fieldsList.map((field) => ({
						...field,
						path: [this.tableName, ...field.path],
					}));
				}
				this.config.fieldsList.push(
					...orderSelectedFields<AnyPgColumn>(
						table instanceof Subquery ? table[SubqueryConfig].selection : table[Table.Symbol.Columns],
						[tableName],
					),
				);
			}

			if (typeof on === 'function') {
				on = on(
					new Proxy(
						this.config.fields,
						new SelectionProxyHandler({ sqlAliasedBehavior: 'alias', sqlBehavior: 'sql' }),
					) as TSelection,
				);
			}

			this.config.joins[tableName] = { on, table, joinType };

			switch (joinType) {
				case 'left':
					this.joinsNotNullableMap[tableName] = false;
					break;
				case 'right':
					this.joinsNotNullableMap = Object.fromEntries(
						Object.entries(this.joinsNotNullableMap).map(([key]) => [key, false]),
					);
					this.joinsNotNullableMap[tableName] = true;
					break;
				case 'inner':
					this.joinsNotNullableMap[tableName] = true;
					break;
				case 'full':
					this.joinsNotNullableMap = Object.fromEntries(
						Object.entries(this.joinsNotNullableMap).map(([key]) => [key, false]),
					);
					this.joinsNotNullableMap[tableName] = false;
					break;
			}

			return this;
		};
	}

	leftJoin = this.createJoin('left');

	rightJoin = this.createJoin('right');

	innerJoin = this.createJoin('inner');

	fullJoin = this.createJoin('full');

	where(where: ((aliases: TSelection) => SQL | undefined) | SQL | undefined) {
		if (typeof where === 'function') {
			where = where(
				new Proxy(
					this.config.fields,
					new SelectionProxyHandler({ sqlAliasedBehavior: 'sql', sqlBehavior: 'sql' }),
				) as TSelection,
			);
		}
		this.config.where = where;
		return this;
	}

	having(having: ((aliases: TSelection) => SQL | undefined) | SQL | undefined) {
		if (typeof having === 'function') {
			having = having(
				new Proxy(
					this.config.fields,
					new SelectionProxyHandler({ sqlAliasedBehavior: 'sql', sqlBehavior: 'sql' }),
				) as TSelection,
			);
		}
		this.config.having = having;
		return this;
	}

	groupBy(builder: (aliases: TSelection) => ValueOrArray<AnyPgColumn | SQL | SQL.Aliased>): this;
	groupBy(...columns: (AnyPgColumn | SQL)[]): this;
	groupBy(
		...columns:
			| [(aliases: TSelection) => ValueOrArray<AnyPgColumn | SQL | SQL.Aliased>]
			| (AnyPgColumn | SQL)[]
	) {
		if (typeof columns[0] === 'function') {
			const groupBy = columns[0](
				new Proxy(
					this.config.fields,
					new SelectionProxyHandler({ sqlAliasedBehavior: 'alias', sqlBehavior: 'sql' }),
				) as TSelection,
			);
			this.config.groupBy = Array.isArray(groupBy) ? groupBy : [groupBy];
		} else {
			this.config.groupBy = columns as (AnyPgColumn | SQL)[];
		}
		return this;
	}

	orderBy(builder: (aliases: TSelection) => ValueOrArray<AnyPgColumn | SQL | SQL.Aliased>): this;
	orderBy(...columns: (AnyPgColumn | SQL)[]): this;
	orderBy(
		...columns:
			| [(aliases: TSelection) => ValueOrArray<AnyPgColumn | SQL | SQL.Aliased>]
			| (AnyPgColumn | SQL)[]
	) {
		if (typeof columns[0] === 'function') {
			const orderBy = columns[0](
				new Proxy(
					this.config.fields,
					new SelectionProxyHandler({ sqlAliasedBehavior: 'alias', sqlBehavior: 'sql' }),
				) as TSelection,
			);
			this.config.orderBy = Array.isArray(orderBy) ? orderBy : [orderBy];
		} else {
			this.config.orderBy = columns as (AnyPgColumn | SQL)[];
		}
		return this;
	}

	limit(limit: number) {
		this.config.limit = limit;
		return this;
	}

	offset(offset: number) {
		this.config.offset = offset;
		return this;
	}

	for(strength: LockStrength, config: LockConfig = {}) {
		this.config.lockingClauses.push({ strength, config });
		return this;
	}

	/** @internal */
	getSQL(): SQL {
		return this.dialect.buildSelectQuery(this.config);
	}

	toSQL(): Simplify<Omit<Query, 'typings'>> {
		const { typings, ...rest } = this.dialect.sqlToQuery(this.getSQL());
		return rest;
	}

	as<TAlias extends string>(
		alias: TAlias,
	): SubqueryWithSelection<BuildSubquerySelection<TSelection, TNullabilityMap>, TAlias> {
		return new Proxy(
			new Subquery(this.getSQL(), this.config.fields, alias),
			new SelectionProxyHandler({ alias, sqlAliasedBehavior: 'alias', sqlBehavior: 'error' }),
		) as SubqueryWithSelection<BuildSubquerySelection<TSelection, TNullabilityMap>, TAlias>;
	}
}

export interface PgSelect<
	TTableName extends string,
	TSelection,
	TSelectMode extends SelectMode,
	TNullabilityMap extends Record<string, JoinNullability> = Record<TTableName, 'not-null'>,
> extends
	PgSelectQueryBuilder<PgSelectHKT, TTableName, TSelection, TSelectMode, TNullabilityMap>,
	QueryPromise<SelectResult<TSelection, TSelectMode, TNullabilityMap>[]>
{}

export class PgSelect<
	TTableName extends string,
	TSelection,
	TSelectMode extends SelectMode,
	TNullabilityMap extends Record<string, JoinNullability> = Record<TTableName, 'not-null'>,
> extends PgSelectQueryBuilder<PgSelectHKT, TTableName, TSelection, TSelectMode, TNullabilityMap> {
	private _prepare(name?: string): PreparedQuery<
		PreparedQueryConfig & {
			execute: SelectResult<TSelection, TSelectMode, TNullabilityMap>[];
		}
	> {
		if (!this.session) {
			throw new Error('Cannot execute a query on a query builder. Please use a database instance instead.');
		}
		const query = this.session.prepareQuery<
			PreparedQueryConfig & { execute: SelectResult<TSelection, TSelectMode, TNullabilityMap>[] }
		>(this.dialect.sqlToQuery(this.getSQL()), this.config.fieldsList, name);
		query.joinsNotNullableMap = this.joinsNotNullableMap;
		return query;
	}

	prepare(name: string): PreparedQuery<
		PreparedQueryConfig & {
			execute: SelectResult<TSelection, TSelectMode, TNullabilityMap>[];
		}
	> {
		return this._prepare(name);
	}

	execute: ReturnType<this['prepare']>['execute'] = (placeholderValues) => {
		return this._prepare().execute(placeholderValues);
	};
}

applyMixins(PgSelect, [QueryPromise]);
