import { TermBuilder, TermOptions, TermType } from './terms';

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
