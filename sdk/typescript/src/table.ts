import {
  BaseRestrictedTerm,
  createRestrictedTerm,
  DeleteQuery,
  FilterQuery,
  GetQuery,
  InsertQuery,
  registerTableMethods,
  RestrictedTerm
} from './builder';
import { Client } from './client';
import { Cursor, CursorOptions } from './cursor';
import { DatabaseDocument, TermArg, TermType } from './types';

export function addTableMethods(term: BaseRestrictedTerm<'table'>): RestrictedTerm<'table'> {
  const tableTerm = term as RestrictedTerm<'table'>;

  tableTerm.get = <T = DatabaseDocument>(key: TermArg): GetQuery<T> => {
    const baseTerm = createRestrictedTerm('query', TermType.Get, [term, key]);
    return Object.assign(baseTerm, {
      run: ((client: Client) =>
        client.send<ReturnType<typeof baseTerm.toAST>, T>(baseTerm.toAST())) as {
        (client: Client): Promise<T>;
        <U>(client: Client): Promise<U>;
      },
      delete: (): DeleteQuery => {
        const deleteTerm = createRestrictedTerm('query', TermType.Delete, [baseTerm]);
        return Object.assign(deleteTerm, {
          run: ((client: Client) =>
            client.send<ReturnType<typeof deleteTerm.toAST>, { deleted: number }>(
              deleteTerm.toAST()
            )) as {
            (client: Client): Promise<{ deleted: number }>;
            <U>(client: Client): Promise<U>;
          },
          debug: (): DeleteQuery => {
            console.dir(deleteTerm.toAST(), { depth: null });
            return tableTerm.get<T>(key).delete();
          }
        }) as DeleteQuery;
      },
      debug: (): GetQuery<T> => {
        console.dir(baseTerm.toAST(), { depth: null });
        return tableTerm.get<T>(key);
      }
    }) as GetQuery<T>;
  };

  tableTerm.filter = <T = DatabaseDocument>(predicate: RestrictedTerm<'expr'>): FilterQuery<T> => {
    const baseTerm = createRestrictedTerm('query', TermType.Filter, [term, predicate]);
    return Object.assign(baseTerm, {
      run: ((client: Client, options?: CursorOptions) =>
        new Cursor<T>(client, baseTerm, options)) as {
        (client: Client, options?: CursorOptions): Cursor<T>;
        <U>(client: Client, options?: CursorOptions): Cursor<U>;
      },
      filter: (nextPredicate: RestrictedTerm<'expr'>): FilterQuery<T> => {
        const newFilterTerm = createRestrictedTerm('query', TermType.Filter, [
          baseTerm,
          nextPredicate
        ]);
        return Object.assign(newFilterTerm, {
          run: ((client: Client, options?: CursorOptions) =>
            new Cursor<T>(client, newFilterTerm, options)) as {
            (client: Client, options?: CursorOptions): Cursor<T>;
            <U>(client: Client, options?: CursorOptions): Cursor<U>;
          },
          filter: (nextNextPredicate: RestrictedTerm<'expr'>): FilterQuery<T> => {
            return tableTerm.filter<T>(nextNextPredicate);
          },
          debug: (): FilterQuery<T> => {
            console.dir(newFilterTerm.toAST(), { depth: null });
            return tableTerm.filter<T>(nextPredicate);
          }
        }) as FilterQuery<T>;
      },
      debug: (): FilterQuery<T> => {
        console.dir(baseTerm.toAST(), { depth: null });
        return tableTerm.filter<T>(predicate);
      }
    }) as FilterQuery<T>;
  };

  tableTerm.insert = <T = DatabaseDocument>(documents: T | T[]): InsertQuery => {
    const baseTerm = createRestrictedTerm('query', TermType.Insert, [term, documents as TermArg]);
    return Object.assign(baseTerm, {
      run: ((client: Client) =>
        client.send<
          ReturnType<typeof baseTerm.toAST>,
          { inserted: number; generated_keys?: string[] }
        >(baseTerm.toAST())) as {
        (client: Client): Promise<{ inserted: number; generated_keys?: string[] }>;
        <T>(client: Client): Promise<T>;
      },
      debug: (): InsertQuery => {
        console.dir(baseTerm.toAST(), { depth: null });
        return tableTerm.insert(documents);
      }
    }) as InsertQuery;
  };

  tableTerm.delete = (): DeleteQuery => {
    const baseTerm = createRestrictedTerm('query', TermType.Delete, [term]);
    return Object.assign(baseTerm, {
      run: ((client: Client) =>
        client.send<ReturnType<typeof baseTerm.toAST>, { deleted: number }>(baseTerm.toAST())) as {
        (client: Client): Promise<{ deleted: number }>;
        <T>(client: Client): Promise<T>;
      },
      debug: (): DeleteQuery => {
        console.dir(baseTerm.toAST(), { depth: null });
        return tableTerm.delete();
      }
    }) as DeleteQuery;
  };

  // Execution methods
  tableTerm.run = <T = DatabaseDocument>(client: Client, options?: CursorOptions): Cursor<T> =>
    new Cursor<T>(client, term, options);

  tableTerm.debug = (): RestrictedTerm<'table'> => {
    console.dir(term.toAST(), { depth: null });
    return tableTerm;
  };

  return tableTerm;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function isTableTerm(term: any): term is RestrictedTerm<'table'> {
  return term && term._context === 'table';
}

// Register methods with the builder
registerTableMethods(addTableMethods);

// Export type for external use
export type TableTerm = RestrictedTerm<'table'>;
