import {
  BaseRestrictedTerm,
  createRestrictedTerm,
  registerExprMethods,
  RestrictedTerm
} from './builder';
import { Client } from './client';
import { TermArg, TermType } from './types';

export function addExprMethods(term: BaseRestrictedTerm<'expr'>): RestrictedTerm<'expr'> {
  const exprTerm = term as RestrictedTerm<'expr'>;

  // Comparison operations
  exprTerm.eq = (value: TermArg) => createRestrictedTerm('expr', TermType.Eq, [term, value]);
  exprTerm.ne = (value: TermArg) => createRestrictedTerm('expr', TermType.Ne, [term, value]);
  exprTerm.lt = (value: TermArg) => createRestrictedTerm('expr', TermType.Lt, [term, value]);
  exprTerm.le = (value: TermArg) => createRestrictedTerm('expr', TermType.Le, [term, value]);
  exprTerm.gt = (value: TermArg) => createRestrictedTerm('expr', TermType.Gt, [term, value]);
  exprTerm.ge = (value: TermArg) => createRestrictedTerm('expr', TermType.Ge, [term, value]);

  // Logical operations
  exprTerm.and = (other: RestrictedTerm<'expr'>) =>
    createRestrictedTerm('expr', TermType.And, [term, other]);
  exprTerm.or = (other: RestrictedTerm<'expr'>) =>
    createRestrictedTerm('expr', TermType.Or, [term, other]);
  exprTerm.not = () => createRestrictedTerm('expr', TermType.Not, [term]);

  // Field access
  exprTerm.getField = (field: TermArg) =>
    createRestrictedTerm('expr', TermType.GetField, [term, field]);

  // Execution methods
  exprTerm.run = async <T = unknown>(client: Client): Promise<T> =>
    await client.send<ReturnType<typeof term.toAST>, T>(term.toAST());

  exprTerm.debug = (): RestrictedTerm<'expr'> => {
    console.dir(term.toAST(), { depth: null });
    return exprTerm;
  };

  return exprTerm;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function isExprTerm(term: any): term is RestrictedTerm<'expr'> {
  return term && term._context === 'expr';
}

// Register methods with the builder
registerExprMethods(addExprMethods);

// Export type for external use
export type ExprTerm = RestrictedTerm<'expr'>;
