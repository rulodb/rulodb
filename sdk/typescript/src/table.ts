import { ExprBuilder } from './expr';
import { DeepGet, DeepKeys, Term, TermBuilder, TermOptions, TermType } from './terms';

type FieldOptions = {
  separator?: string;
} & TermOptions;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export class TableBuilder<T extends object = any> extends TermBuilder<T> {
  constructor(name: string, database: string | Term = 'default', options: TermOptions = {}) {
    super(
      TermType.Table,
      [typeof database === 'string' ? [TermType.Database, [database]] : database, name],
      options
    );
  }

  field<K extends DeepKeys<T> & string>(
    field: K,
    optArgs: FieldOptions = {}
  ): ExprBuilder<T, DeepGet<T, K>> {
    return new ExprBuilder<T, DeepGet<T, K>>(TermType.GetField, [field], optArgs);
  }

  insert(docs: T | T[], optArgs: TermOptions = {}): TermBuilder<T> {
    const docsArray = Array.isArray(docs) ? docs : [docs];
    return new TermBuilder<T>(TermType.Insert, [this.build(), docsArray], optArgs);
  }

  get(id: string, optArgs: TermOptions = {}): TermBuilder<T> {
    return new TermBuilder<T>(TermType.Get, [this.build(), id], optArgs);
  }

  filter(predicate: ExprBuilder<Partial<T>>, optArgs: TermOptions = {}): TermBuilder<T> {
    return new TermBuilder<T>(TermType.Filter, [this.build(), predicate.build()], optArgs);
  }
}

export function table<T extends object = Document>(
  ...args: ConstructorParameters<typeof TableBuilder<T>>
): TableBuilder<T> {
  return new TableBuilder<T>(...args);
}
