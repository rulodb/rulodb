import { expr, ExprBuilder, row } from '../src/expr';
import { TermType } from '../src/terms';

describe('ExprBuilder', () => {
  it('should build a row expr', () => {
    const rowExpr = row('age');
    expect(rowExpr).toBeInstanceOf(ExprBuilder);
    expect(rowExpr.build()).toEqual([TermType.GetField, ['age']]);
  });

  it('should build a row expr with optArgs', () => {
    const rowExpr = row('age', { separator: ',' });
    expect(rowExpr).toBeInstanceOf(ExprBuilder);
    expect(rowExpr.build()).toEqual([TermType.GetField, ['age'], { separator: ',' }]);
  });

  it('should build a basic expr term', () => {
    const builder = new ExprBuilder(TermType.Expr, [{ foo: 1 }]);
    expect(builder.build()).toEqual([TermType.Expr, [{ foo: 1 }]]);
  });

  it('should build eq term', () => {
    const builder = new ExprBuilder(TermType.GetField, ['foo']);
    const eq = builder.eq(2);
    expect(eq.build()).toEqual([
      TermType.Eq,
      [
        [TermType.GetField, ['foo']],
        [TermType.Datum, [2]]
      ]
    ]);
  });

  it('should not double-wrap ExprBuilder in expr()', () => {
    const builder = new ExprBuilder(TermType.GetField, ['foo']);
    expect(expr(builder)).toBe(builder);
  });

  it('should wrap non-ExprBuilder in expr()', () => {
    const value = { foo: 1 };
    const wrapped = expr(value);
    expect(wrapped).toBeInstanceOf(ExprBuilder);
    expect(wrapped.build()).toEqual([TermType.Expr, [value]]);
  });

  it('should build expression with expression values', () => {
    const builder = new ExprBuilder(TermType.GetField, ['foo']);
    expect(builder.eq(0).build()).toEqual([
      TermType.Eq,
      [
        [TermType.GetField, ['foo']],
        [TermType.Datum, [0]]
      ]
    ]);
  });

  it('should build eq expression', () => {
    const builder = new ExprBuilder(TermType.GetField, ['foo']);
    expect(builder.eq(0).build()).toEqual([
      TermType.Eq,
      [
        [TermType.GetField, ['foo']],
        [TermType.Datum, [0]]
      ]
    ]);
  });

  it('should build neq expression', () => {
    const builder = new ExprBuilder(TermType.GetField, ['foo']);
    expect(builder.ne(0).build()).toEqual([
      TermType.Ne,
      [
        [TermType.GetField, ['foo']],
        [TermType.Datum, [0]]
      ]
    ]);
  });

  it('should build lt expression', () => {
    const builder = new ExprBuilder(TermType.GetField, ['foo']);
    expect(builder.lt(0).build()).toEqual([
      TermType.Lt,
      [
        [TermType.GetField, ['foo']],
        [TermType.Datum, [0]]
      ]
    ]);
  });

  it('should build le expression', () => {
    const builder = new ExprBuilder(TermType.GetField, ['foo']);
    expect(builder.le(0).build()).toEqual([
      TermType.Le,
      [
        [TermType.GetField, ['foo']],
        [TermType.Datum, [0]]
      ]
    ]);
  });

  it('should build ge expression', () => {
    const builder = new ExprBuilder(TermType.GetField, ['foo']);
    expect(builder.ge(0).build()).toEqual([
      TermType.Ge,
      [
        [TermType.GetField, ['foo']],
        [TermType.Datum, [0]]
      ]
    ]);
  });

  it('should build gt expression', () => {
    const builder = new ExprBuilder(TermType.GetField, ['foo']);
    expect(builder.gt(0).build()).toEqual([
      TermType.Gt,
      [
        [TermType.GetField, ['foo']],
        [TermType.Datum, [0]]
      ]
    ]);
  });

  it('should build not expression', () => {
    const builder = new ExprBuilder(TermType.GetField, ['foo']);
    expect(builder.not(0).build()).toEqual([
      TermType.Not,
      [
        [TermType.GetField, ['foo']],
        [TermType.Datum, [0]]
      ]
    ]);
  });

  it('should build and expression', () => {
    const builder = new ExprBuilder(TermType.GetField, ['foo']);
    expect(builder.and(true).build()).toEqual([
      TermType.And,
      [
        [TermType.GetField, ['foo']],
        [TermType.Datum, [true]]
      ]
    ]);
  });

  it('should build or expression', () => {
    const builder = new ExprBuilder(TermType.GetField, ['foo']);
    expect(builder.or(true).build()).toEqual([
      TermType.Or,
      [
        [TermType.GetField, ['foo']],
        [TermType.Datum, [true]]
      ]
    ]);
  });
});
