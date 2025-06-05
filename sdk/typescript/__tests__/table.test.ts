import { ExprBuilder } from '../src/expr';
import { table, TableBuilder } from '../src/table';
import { TermType } from '../src/terms';

describe('TableBuilder', () => {
  it('should build a table term', () => {
    const builder = new TableBuilder('users');
    expect(builder.build()).toEqual([
      TermType.Table,
      [[TermType.Database, ['default']], 'users'],
      {}
    ]);
  });

  it('should build an insert term', () => {
    const builder = new TableBuilder('users');
    const docs = [{ name: 'Alice', age: 30 }];
    const insert = builder.insert(docs);
    expect(insert.build()).toEqual([
      TermType.Insert,
      [[TermType.Table, [[TermType.Database, ['default']], 'users'], {}], docs],
      {}
    ]);
  });

  it('should build a row expr', () => {
    const builder = new TableBuilder('users');
    const rowExpr = builder.row('age');
    expect(rowExpr).toBeInstanceOf(ExprBuilder);
    expect(rowExpr.build()).toEqual([TermType.GetField, ['age']]);
  });

  it('should build a get term', () => {
    const builder = new TableBuilder('users');
    const get = builder.get('id123');
    expect(get.build()).toEqual([
      TermType.Get,
      [[TermType.Table, [[TermType.Database, ['default']], 'users'], {}], 'id123'],
      {}
    ]);
  });

  it('should build a filter term', () => {
    const builder = new TableBuilder('users');
    const predicate = builder.row('age').ge(21);
    const filter = builder.filter(predicate);
    expect(filter.build()).toEqual([
      TermType.Filter,
      [
        [TermType.Table, [[TermType.Database, ['default']], 'users'], {}],
        [
          TermType.Ge,
          [
            [TermType.GetField, ['age']],
            [TermType.Datum, [21]]
          ]
        ]
      ],
      {}
    ]);
  });

  it('table() factory should return TableBuilder', () => {
    const t = table('users');
    expect(t).toBeInstanceOf(TableBuilder);
    expect(t.build()).toEqual([TermType.Table, [[TermType.Database, ['default']], 'users'], {}]);
  });
});
