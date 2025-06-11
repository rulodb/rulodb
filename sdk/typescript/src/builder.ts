import { Client } from './client';
import { Cursor, CursorOptions } from './cursor';
import { DatabaseDocument, GetFieldOptions, Term, TermArg, TermOptions, TermType } from './types';

// Utility type for deep object key paths
export type DeepKeys<T, D extends number = 5> = [D] extends [0]
  ? never
  : {
      [K in keyof T & (string | number)]: T[K] extends object
        ? K | `${K}.${DeepKeys<T[K], [never, 0, 1, 2, 3, 4, 5][D]>}`
        : K;
    }[keyof T & (string | number)];

// Helper function to recursively resolve TermArgs to their AST representations
function resolveTermArg(arg: TermArg): TermArg {
  if (arg && typeof arg === 'object' && 'toAST' in arg && typeof arg.toAST === 'function') {
    // This is a Term, resolve it to its AST
    return (arg as Term).toAST();
  } else if (Array.isArray(arg)) {
    // Recursively resolve array elements
    return arg.map(resolveTermArg);
  }
  // Return primitive values as-is
  return arg;
}

// Create a term with the given type and arguments
export function createTerm(type: TermType, args: TermArg[], options?: TermOptions): Term {
  return {
    toAST() {
      // Recursively resolve all arguments
      const resolvedArgs = args.map(resolveTermArg);
      return options ? [type, resolvedArgs, options] : [type, resolvedArgs];
    }
  };
}

// Chain context types - determines what operations are available
export type ChainContext = 'root' | 'database' | 'table' | 'query' | 'expr';

// Base restricted term with context awareness
export interface BaseRestrictedTerm<TContext extends ChainContext = ChainContext> extends Term {
  readonly _context: TContext;
}

// Specific interfaces for operations with known return types - RethinkDB-TS approach
export interface ListTablesQuery extends BaseRestrictedTerm<'table'> {
  run(client: Client): Promise<string[]>;
  run<T>(client: Client): Promise<T>;
  debug(): ListTablesQuery;
}

export interface CreateTableQuery extends BaseRestrictedTerm<'table'> {
  run(client: Client): Promise<{ created: boolean }>;
  run<T>(client: Client): Promise<T>;
  debug(): CreateTableQuery;
}

export interface DropTableQuery extends BaseRestrictedTerm<'table'> {
  run(client: Client): Promise<{ dropped: boolean }>;
  run<T>(client: Client): Promise<T>;
  debug(): DropTableQuery;
}

export interface GetQuery<T = DatabaseDocument> extends BaseRestrictedTerm<'query'> {
  run(client: Client): Promise<T>;
  run<U>(client: Client): Promise<U>;
  delete(): DeleteQuery;
  debug(): GetQuery<T>;
}

export interface FilterQuery<T = DatabaseDocument> extends BaseRestrictedTerm<'query'> {
  run(client: Client, options?: CursorOptions): Cursor<T>;
  run<U>(client: Client, options?: CursorOptions): Cursor<U>;
  filter(predicate: RestrictedTerm<'expr'>): FilterQuery<T>;
  debug(): FilterQuery<T>;
}

export interface InsertQuery extends BaseRestrictedTerm<'query'> {
  run(client: Client): Promise<{ inserted: number; generated_keys?: string[] }>;
  run<T>(client: Client): Promise<T>;
  debug(): InsertQuery;
}

export interface DeleteQuery extends BaseRestrictedTerm<'query'> {
  run(client: Client): Promise<{ deleted: number }>;
  run<T>(client: Client): Promise<T>;
  debug(): DeleteQuery;
}

// Forward declare method interfaces to avoid circular imports
export interface RootMethods {
  db(name?: TermArg): RestrictedTerm<'database'>;
  listDatabases(): RestrictedTerm<'database'>;
  createDatabase(name: TermArg): RestrictedTerm<'database'>;
  dropDatabase(name: TermArg): RestrictedTerm<'database'>;
  expr(args: TermArg[]): RestrictedTerm<'expr'>;
  row<T extends import('./types').DatabaseDocument>(
    field: DeepKeys<T> & string,
    options?: GetFieldOptions
  ): RestrictedTerm<'expr'>;
  eq(left: TermArg, right: TermArg): RestrictedTerm<'expr'>;
  ne(left: TermArg, right: TermArg): RestrictedTerm<'expr'>;
  lt(left: TermArg, right: TermArg): RestrictedTerm<'expr'>;
  le(left: TermArg, right: TermArg): RestrictedTerm<'expr'>;
  gt(left: TermArg, right: TermArg): RestrictedTerm<'expr'>;
  ge(left: TermArg, right: TermArg): RestrictedTerm<'expr'>;
  and(left: RestrictedTerm<'expr'>, right: RestrictedTerm<'expr'>): RestrictedTerm<'expr'>;
  or(left: RestrictedTerm<'expr'>, right: RestrictedTerm<'expr'>): RestrictedTerm<'expr'>;
  not(term: RestrictedTerm<'expr'>): RestrictedTerm<'expr'>;
}

export interface DatabaseMethods {
  table(name: TermArg): RestrictedTerm<'table'>;
  listTables(): ListTablesQuery;
  createTable(name: TermArg): CreateTableQuery;
  dropTable(name: TermArg): DropTableQuery;
  run<T = unknown>(client: Client): Promise<T>;
  debug(): RestrictedTerm<'database'>;
}

export interface TableMethods {
  get(key: TermArg): GetQuery<DatabaseDocument>;
  get<T>(key: TermArg): GetQuery<T>;
  filter(predicate: RestrictedTerm<'expr'>): FilterQuery<DatabaseDocument>;
  filter<T>(predicate: RestrictedTerm<'expr'>): FilterQuery<T>;
  insert<T = DatabaseDocument>(documents: T | T[]): InsertQuery;
  delete(): DeleteQuery;
  run<T = DatabaseDocument>(client: Client, options?: CursorOptions): Cursor<T>;
  debug(): RestrictedTerm<'table'>;
}

export interface QueryMethods {
  filter(predicate: RestrictedTerm<'expr'>): RestrictedTerm<'query'>;
  delete(): RestrictedTerm<'query'>;
  run<T = unknown>(client: Client, options?: CursorOptions): Cursor<T> | Promise<T>;
  debug(): RestrictedTerm<'query'>;
}

export interface ExprMethods {
  eq(value: TermArg): RestrictedTerm<'expr'>;
  ne(value: TermArg): RestrictedTerm<'expr'>;
  lt(value: TermArg): RestrictedTerm<'expr'>;
  le(value: TermArg): RestrictedTerm<'expr'>;
  gt(value: TermArg): RestrictedTerm<'expr'>;
  ge(value: TermArg): RestrictedTerm<'expr'>;
  and(other: RestrictedTerm<'expr'>): RestrictedTerm<'expr'>;
  or(other: RestrictedTerm<'expr'>): RestrictedTerm<'expr'>;
  not(): RestrictedTerm<'expr'>;
  getField(field: TermArg): RestrictedTerm<'expr'>;
  run<T = unknown>(client: Client): Promise<T>;
  debug(): RestrictedTerm<'expr'>;
}

// Conditional type that determines available methods based on context
// prettier-ignore
export type ContextualMethods<TContext extends ChainContext> =
  TContext extends 'root' ? RootMethods :
  TContext extends 'database' ? DatabaseMethods :
  TContext extends 'table' ? TableMethods :
  TContext extends 'query' ? QueryMethods :
  TContext extends 'expr' ? ExprMethods :
  never;

// Main restricted term type that combines context with methods
export type RestrictedTerm<TContext extends ChainContext> = BaseRestrictedTerm<TContext> &
  ContextualMethods<TContext>;

// Store method implementations
const methodImplementations: {
  root?: (term: BaseRestrictedTerm<'root'>) => RestrictedTerm<'root'>;
  database?: (term: BaseRestrictedTerm<'database'>) => RestrictedTerm<'database'>;
  table?: (term: BaseRestrictedTerm<'table'>) => RestrictedTerm<'table'>;
  query?: (term: BaseRestrictedTerm<'query'>) => RestrictedTerm<'query'>;
  expr?: (term: BaseRestrictedTerm<'expr'>) => RestrictedTerm<'expr'>;
} = {};

// Registration functions for method implementations
export function registerRootMethods(
  impl: (term: BaseRestrictedTerm<'root'>) => RestrictedTerm<'root'>
) {
  methodImplementations.root = impl;
}
export function registerDatabaseMethods(
  impl: (term: BaseRestrictedTerm<'database'>) => RestrictedTerm<'database'>
) {
  methodImplementations.database = impl;
}
export function registerTableMethods(
  impl: (term: BaseRestrictedTerm<'table'>) => RestrictedTerm<'table'>
) {
  methodImplementations.table = impl;
}
export function registerQueryMethods(
  impl: (term: BaseRestrictedTerm<'query'>) => RestrictedTerm<'query'>
) {
  methodImplementations.query = impl;
}
export function registerExprMethods(
  impl: (term: BaseRestrictedTerm<'expr'>) => RestrictedTerm<'expr'>
) {
  methodImplementations.expr = impl;
}

// Implementation factory function
export function createRestrictedTerm<TContext extends ChainContext>(
  context: TContext,
  type: TermType,
  args: TermArg[],
  options?: TermOptions
): RestrictedTerm<TContext> {
  const baseTerm = createTerm(type, args, options);
  const term = Object.assign(baseTerm, { _context: context }) as BaseRestrictedTerm<TContext>;

  // Add methods based on context
  switch (context) {
    case 'root':
      if (!methodImplementations.root) throw new Error('Root methods not registered');
      return methodImplementations.root(
        term as unknown as BaseRestrictedTerm<'root'>
      ) as unknown as RestrictedTerm<TContext>;
    case 'database':
      if (!methodImplementations.database) throw new Error('Database methods not registered');
      return methodImplementations.database(
        term as unknown as BaseRestrictedTerm<'database'>
      ) as unknown as RestrictedTerm<TContext>;
    case 'table':
      if (!methodImplementations.table) throw new Error('Table methods not registered');
      return methodImplementations.table(
        term as unknown as BaseRestrictedTerm<'table'>
      ) as unknown as RestrictedTerm<TContext>;
    case 'query':
      if (!methodImplementations.query) throw new Error('Query methods not registered');
      return methodImplementations.query(
        term as unknown as BaseRestrictedTerm<'query'>
      ) as unknown as RestrictedTerm<TContext>;
    case 'expr':
      if (!methodImplementations.expr) throw new Error('Expr methods not registered');
      return methodImplementations.expr(
        term as unknown as BaseRestrictedTerm<'expr'>
      ) as unknown as RestrictedTerm<TContext>;
    default:
      throw new Error(`Unknown context: ${context}`);
  }
}
