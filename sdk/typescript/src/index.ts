// Import all modules to ensure method registration happens
import './database';
import './table';
import './query';
import './expression';
import './cursor';

import {
  BaseRestrictedTerm,
  createRestrictedTerm,
  DeepKeys,
  registerRootMethods,
  RestrictedTerm
} from './builder';
import { GetFieldOptions, TermArg, TermType } from './types';
import { DatabaseDocument } from './types';

export function addRootMethods(term: BaseRestrictedTerm<'root'>): RestrictedTerm<'root'> {
  const rootTerm = term as RestrictedTerm<'root'>;

  // Database operations
  rootTerm.db = (name: TermArg = 'default') =>
    createRestrictedTerm('database', TermType.Database, [name]);
  rootTerm.listDatabases = () => createRestrictedTerm('database', TermType.DatabaseList, []);
  rootTerm.createDatabase = (name: TermArg) =>
    createRestrictedTerm('database', TermType.DatabaseCreate, [name]);
  rootTerm.dropDatabase = (name: TermArg) =>
    createRestrictedTerm('database', TermType.DatabaseDrop, [name]);

  // Expression operations
  rootTerm.expr = (args: TermArg[]) => createRestrictedTerm('expr', TermType.Expr, args);
  rootTerm.row = <T extends DatabaseDocument>(
    field: DeepKeys<T> & string,
    options?: GetFieldOptions
  ) => createRestrictedTerm('expr', TermType.GetField, [field], options);
  rootTerm.eq = (left: TermArg, right: TermArg) =>
    createRestrictedTerm('expr', TermType.Eq, [left, right]);
  rootTerm.ne = (left: TermArg, right: TermArg) =>
    createRestrictedTerm('expr', TermType.Ne, [left, right]);
  rootTerm.lt = (left: TermArg, right: TermArg) =>
    createRestrictedTerm('expr', TermType.Lt, [left, right]);
  rootTerm.le = (left: TermArg, right: TermArg) =>
    createRestrictedTerm('expr', TermType.Le, [left, right]);
  rootTerm.gt = (left: TermArg, right: TermArg) =>
    createRestrictedTerm('expr', TermType.Gt, [left, right]);
  rootTerm.ge = (left: TermArg, right: TermArg) =>
    createRestrictedTerm('expr', TermType.Ge, [left, right]);
  rootTerm.and = (left: RestrictedTerm<'expr'>, right: RestrictedTerm<'expr'>) =>
    createRestrictedTerm('expr', TermType.And, [left, right]);
  rootTerm.or = (left: RestrictedTerm<'expr'>, right: RestrictedTerm<'expr'>) =>
    createRestrictedTerm('expr', TermType.Or, [left, right]);
  rootTerm.not = (termArg: RestrictedTerm<'expr'>) =>
    createRestrictedTerm('expr', TermType.Not, [termArg]);

  return rootTerm;
}

// Main builder function
export function createRestrictedBuilder(): RestrictedTerm<'root'> {
  return createRestrictedTerm('root', TermType.Expr, []);
}

// Register methods with the builder
registerRootMethods(addRootMethods);

// Create the main builder instance for export
export const r = createRestrictedBuilder();

// Export type for external use
export type RootTerm = RestrictedTerm<'root'>;

// Re-export all types and functions for convenience
export type { DEFAULT_POOL_OPTIONS } from './client';
export { Client } from './client';
export type { CursorOptions } from './cursor';
export { Cursor, isCursor } from './cursor';
export type { DatabaseTerm } from './database';
export { isDatabaseTerm } from './database';
export type { ExprTerm } from './expression';
export { isExprTerm } from './expression';
export type { QueryTerm } from './query';
export { isQueryTerm } from './query';
export type { TableTerm } from './table';
export { isTableTerm } from './table';

// Export types from builder
export type {
  BaseRestrictedTerm,
  ChainContext,
  CreateTableQuery,
  DeleteQuery,
  DropTableQuery,
  FilterQuery,
  GetQuery,
  InsertQuery,
  ListTablesQuery,
  RestrictedTerm
} from './builder';

// Export types from types
export type {
  DatabaseDocument,
  GetFieldOptions,
  MandatoryField,
  Term,
  TermArg,
  TermAST,
  TermOptions,
  TermPrimitive
} from './types';
export { TermType } from './types';
