import type { GetColumnData } from '~/column.ts';
import { is } from '~/entity.ts';
import { Param, SQL, sql } from '~/sql/sql.ts';
import type { UpdateSet } from '~/utils.ts';
import { PgArray, type PgColumn } from '../columns/common.ts';
import { and, or } from '../expressions.ts';
import { PgTable } from '../table.ts';
import { makePgArray } from './array';

export function mapUpdateSetAfterUnnest(
	table: PgTable,
	values: Record<string, unknown>,
) {
	const columnMap = table[PgTable.Symbol.Columns];
	const entries: [string, UpdateSet[string]][] = Object.entries(values)
		.filter((v) => v !== undefined)
		.map(([key, value]) => {
			// eslint-disable-next-line unicorn/prefer-ternary
			if (is(value, SQL)) {
				return [key, value];
			} else if (Array.isArray(value)) {
				return [key, getUnnestParam(columnMap[key], value)];
			} else {
				return [key, new Param(value, columnMap[key])];
			}
		});

	if (entries.length === 0) {
		throw new Error('No values to set');
	}

	return Object.fromEntries(entries);
}

export function getUnnestParam(
	t: PgColumn | undefined,
	value: unknown[],
) {
	const raw = value.map((v) =>
		v === null
			? null
			: is(t, PgArray)
			? t.mapToDriverValue(v as unknown[], true)
			: t?.mapToDriverValue(v)
	);
	let colType: SQL | undefined = undefined;
	if (t) colType = sql`::${sql.raw(t?.getSQLType())}[]`;
	return sql`${makePgArray(raw)}${colType}`;
}

export interface BinaryOperatorUnnest {
	<TColumn extends PgColumn, V extends GetColumnData<TColumn, 'raw'>[]>(
		left: TColumn,
		right: V,
	): [SQL | undefined, { col: TColumn; val: V }[]];
}

/**
 * Overload of `eq` for setAfterUnnest query.
 */
export const eqUnnest: BinaryOperatorUnnest = <T extends PgColumn, V extends GetColumnData<PgColumn, 'raw'>[]>(
	left: T,
	right: V,
): [SQL, { col: T; val: V }[]] => {
	return [
		sql`${left} = __lib_unnest_input__.${sql.identifier(left.name)}`,
		[{ col: left, val: right }],
	];
};

export const andUnnest = (...binaryOps: ReturnType<BinaryOperatorUnnest>[]): ReturnType<BinaryOperatorUnnest> => {
	const batchedArr: { col: PgColumn; val: GetColumnData<PgColumn, 'raw'>[] }[] = [];
	const andSql = and(...binaryOps.map((v) => {
		batchedArr.push(...v[1]);
		return v[0];
	}));
	return [andSql, batchedArr];
};

export const orUnnest = (...binaryOps: ReturnType<BinaryOperatorUnnest>[]): ReturnType<BinaryOperatorUnnest> => {
	const batchedArr: { col: PgColumn; val: GetColumnData<PgColumn, 'raw'>[] }[] = [];
	const orSql = or(...binaryOps.map((v) => {
		batchedArr.push(...v[1]);
		return v[0];
	}));
	return [orSql, batchedArr];
};
