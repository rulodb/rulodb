import { DeepGet, DeepKeys, TermBuilder, TermOptions, TermType } from './terms';

export type FieldOptions = {
  separator?: string;
} & TermOptions;

export class ExprBuilder<T = Record<string, unknown>, V = unknown> extends TermBuilder<T> {
  constructor(type: TermType, args: unknown[], optArgs: TermOptions = {}) {
    super(type, args, optArgs);
  }

  protected getTerm(termType: TermType, value: V) {
    const unwrap = (v: unknown) => {
      if (v instanceof ExprBuilder) {
        return v.build();
      }

      if (
        v === null ||
        v === undefined ||
        typeof v === 'string' ||
        typeof v === 'number' ||
        typeof v === 'boolean'
      ) {
        return [TermType.Datum, [v]];
      }

      return [TermType.Expr, [v]];
    };

    return [termType, [unwrap(this), unwrap(value)]];
  }

  eq(value: V): ExprBuilder<T> {
    const term = this.getTerm(TermType.Eq, value);
    return new ExprBuilder<T>(term[0] as TermType, term[1] as unknown[]);
  }

  ne(value: V): ExprBuilder<T> {
    const term = this.getTerm(TermType.Ne, value);
    return new ExprBuilder<T>(term[0] as TermType, term[1] as unknown[]);
  }

  lt(value: V): ExprBuilder<T> {
    const term = this.getTerm(TermType.Lt, value);
    return new ExprBuilder<T>(term[0] as TermType, term[1] as unknown[]);
  }

  le(value: V): ExprBuilder<T> {
    const term = this.getTerm(TermType.Le, value);
    return new ExprBuilder<T>(term[0] as TermType, term[1] as unknown[]);
  }

  gt(value: V): ExprBuilder<T> {
    const term = this.getTerm(TermType.Gt, value);
    return new ExprBuilder<T>(term[0] as TermType, term[1] as unknown[]);
  }

  ge(value: V): ExprBuilder<T> {
    const term = this.getTerm(TermType.Ge, value);
    return new ExprBuilder<T>(term[0] as TermType, term[1] as unknown[]);
  }

  not(value: V): ExprBuilder<T> {
    const term = this.getTerm(TermType.Not, value);
    return new ExprBuilder<T>(term[0] as TermType, term[1] as unknown[]);
  }

  and(value: V): ExprBuilder<T> {
    const term = this.getTerm(TermType.And, value);
    return new ExprBuilder<T>(term[0] as TermType, term[1] as unknown[]);
  }

  or(value: V): ExprBuilder<T> {
    const term = this.getTerm(TermType.Or, value);
    return new ExprBuilder<T>(term[0] as TermType, term[1] as unknown[]);
  }
}

export function expr<T = Record<string, unknown>, V = unknown>(value: V): ExprBuilder<T> {
  // Avoid double-wrapping if already an ExprBuilder
  if (value instanceof ExprBuilder) {
    return value;
  }
  return new ExprBuilder<T>(TermType.Expr, [value], {});
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function row<T extends object = any, K extends DeepKeys<T> & string = any>(
  field: K,
  optArgs: FieldOptions = {}
): ExprBuilder<T, DeepGet<T, K>> {
  return new ExprBuilder<T, DeepGet<T, K>>(TermType.GetField, [field], optArgs);
}
