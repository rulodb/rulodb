import {
  BaseRestrictedTerm,
  createRestrictedTerm,
  CreateTableQuery,
  DropTableQuery,
  ListTablesQuery,
  registerDatabaseMethods,
  RestrictedTerm
} from './builder';
import { Client } from './client';
import { TermArg, TermType } from './types';

export function addDatabaseMethods(
  term: BaseRestrictedTerm<'database'>
): RestrictedTerm<'database'> {
  const dbTerm = term as RestrictedTerm<'database'>;

  dbTerm.table = (name: TermArg) => createRestrictedTerm('table', TermType.Table, [term, name]);

  dbTerm.listTables = (): ListTablesQuery => {
    const baseTerm = createRestrictedTerm('table', TermType.TableList, [term]);
    return Object.assign(baseTerm, {
      run: ((client: Client) =>
        client.send<ReturnType<typeof baseTerm.toAST>, string[]>(baseTerm.toAST())) as {
        (client: Client): Promise<string[]>;
        <T>(client: Client): Promise<T>;
      },
      debug: (): ListTablesQuery => {
        console.dir(baseTerm.toAST(), { depth: null });
        return dbTerm.listTables();
      }
    }) as ListTablesQuery;
  };

  dbTerm.createTable = (name: TermArg): CreateTableQuery => {
    const baseTerm = createRestrictedTerm('table', TermType.TableCreate, [term, name]);
    return Object.assign(baseTerm, {
      run: ((client: Client) =>
        client.send<ReturnType<typeof baseTerm.toAST>, { created: boolean }>(baseTerm.toAST())) as {
        (client: Client): Promise<{ created: boolean }>;
        <T>(client: Client): Promise<T>;
      },
      debug: (): CreateTableQuery => {
        console.dir(baseTerm.toAST(), { depth: null });
        return dbTerm.createTable(name);
      }
    }) as CreateTableQuery;
  };

  dbTerm.dropTable = (name: TermArg): DropTableQuery => {
    const baseTerm = createRestrictedTerm('table', TermType.TableDrop, [term, name]);
    return Object.assign(baseTerm, {
      run: ((client: Client) =>
        client.send<ReturnType<typeof baseTerm.toAST>, { dropped: boolean }>(baseTerm.toAST())) as {
        (client: Client): Promise<{ dropped: boolean }>;
        <T>(client: Client): Promise<T>;
      },
      debug: (): DropTableQuery => {
        console.dir(baseTerm.toAST(), { depth: null });
        return dbTerm.dropTable(name);
      }
    }) as DropTableQuery;
  };

  // Execution methods
  dbTerm.run = async <T = unknown>(client: Client): Promise<T> =>
    await client.send<ReturnType<typeof term.toAST>, T>(term.toAST());

  dbTerm.debug = (): RestrictedTerm<'database'> => {
    console.dir(term.toAST(), { depth: null });
    return dbTerm;
  };

  return dbTerm;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function isDatabaseTerm(term: any): term is RestrictedTerm<'database'> {
  return term && term._context === 'database';
}

// Register methods with the builder
registerDatabaseMethods(addDatabaseMethods);

// Export type for external use
export type DatabaseTerm = RestrictedTerm<'database'>;
