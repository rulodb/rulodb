import { ExprBuilder } from './expr';
import { DeepGet, DeepKeys, Term, TermBuilder, TermOptions, TermType } from './terms';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export class TableBuilder<T extends object = any> extends TermBuilder<T> {
  constructor(name: string, database: string | Term = 'default', options: TermOptions = {}) {
    super(
      TermType.Table,
      [typeof database === 'string' ? [TermType.Database, [database]] : database, name],
      options
    );
  }

  insert(docs: T | T[], optargs: TermOptions = {}): TermBuilder<T> {
    const docsArray = Array.isArray(docs) ? docs : [docs];
    return new TermBuilder<T>(TermType.Insert, [this.build(), docsArray], optargs);
  }

  row<K extends DeepKeys<T> & string>(field: K): ExprBuilder<T, DeepGet<T, K>> {
    return new ExprBuilder<T, DeepGet<T, K>>(TermType.GetField, [field]);
  }

  get(id: string, optargs: TermOptions = {}): TermBuilder<T> {
    return new TermBuilder<T>(TermType.Get, [this.build(), id], optargs);
  }

  filter(predicate: ExprBuilder<Partial<T>>, optargs: TermOptions = {}): TermBuilder<T> {
    return new TermBuilder<T>(TermType.Filter, [this.build(), predicate.build()], optargs);
  }
}

export function table<T extends object = Document>(
  ...args: ConstructorParameters<typeof TableBuilder<T>>
): TableBuilder<T> {
  return new TableBuilder<T>(...args);
}
