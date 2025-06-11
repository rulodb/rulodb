import {
  BaseRestrictedTerm,
  createRestrictedTerm,
  registerQueryMethods,
  RestrictedTerm
} from './builder';
import { Client } from './client';
import { Cursor, CursorOptions } from './cursor';
import { TermType } from './types';

export function addQueryMethods(term: BaseRestrictedTerm<'query'>): RestrictedTerm<'query'> {
  const queryTerm = term as RestrictedTerm<'query'>;

  // Query refinement
  queryTerm.filter = (predicate: RestrictedTerm<'expr'>) =>
    createRestrictedTerm('query', TermType.Filter, [term, predicate]);

  // Execution methods
  queryTerm.run = <T = unknown>(
    client: Client,
    options?: CursorOptions
  ): Cursor<T> | Promise<T> => {
    const ast = term.toAST();
    const [termType] = ast;

    // Return cursor for operations that could return large result sets
    if (termType === TermType.Filter || termType === TermType.Table) {
      return new Cursor<T>(client, term, options);
    }

    // Return direct result for single operations like Get, Insert, Delete
    return client.send<ReturnType<typeof term.toAST>, T>(term.toAST());
  };

  queryTerm.debug = (): RestrictedTerm<'query'> => {
    console.dir(term.toAST(), { depth: null });
    return queryTerm;
  };

  return queryTerm;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function isQueryTerm(term: any): term is RestrictedTerm<'query'> {
  return term && term._context === 'query';
}

// Register methods with the builder
registerQueryMethods(addQueryMethods);

// Export type for external use
export type QueryTerm = RestrictedTerm<'query'>;
