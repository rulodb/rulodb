import { Client } from '../src/client';
import { ExprBuilder } from '../src/expr';
import { QueryResult } from '../src/result';
import { table, TableBuilder } from '../src/table';
import { ExecutionResult, TermType } from '../src/terms';

interface MockClient extends Pick<Client, 'send'> {
  send: jest.Mock;
}

describe('TableBuilder', () => {
  it('should build a table term', () => {
    const builder = new TableBuilder('users');
    expect(builder.build()).toEqual([TermType.Table, [[TermType.Database, ['default']], 'users']]);
  });

  it('should build an insert term', () => {
    const builder = new TableBuilder('users');
    const docs = [{ name: 'Alice', age: 30 }];
    const insert = builder.insert(docs);
    expect(insert.build()).toEqual([
      TermType.Insert,
      [[TermType.Table, [[TermType.Database, ['default']], 'users']], docs]
    ]);
  });

  it('should execute insert operation and return QueryResult', async () => {
    const builder = new TableBuilder<{ name: string; age: number }>('users');
    const docs = [{ name: 'Alice', age: 30 }];
    const insert = builder.insert(docs);

    const mockResponse: ExecutionResult<Array<{ id: string; name: string; age: number }>> = {
      result: [{ id: '123', name: 'Alice', age: 30 }],
      stats: {
        read_count: 0,
        inserted_count: 1,
        updated_count: 0,
        deleted_count: 0,
        error_count: 0,
        duration_ms: 10
      }
    };

    const client: MockClient = { send: jest.fn().mockResolvedValue(mockResponse) };
    const result = await insert.run(client as unknown as Client);
    expect(result).toBeInstanceOf(QueryResult);
    expect(result.isImmediate).toBe(true);
    expect(result.result).toEqual([{ id: '123', name: 'Alice', age: 30 }]);
    expect(client.send).toHaveBeenCalled();
  });

  it('should build a row expr on table', () => {
    const builder = new TableBuilder('users');
    const rowExpr = builder.row('age');
    expect(rowExpr).toBeInstanceOf(ExprBuilder);
    expect(rowExpr.build()).toEqual([TermType.GetField, ['age']]);
  });

  it('should build a row expr on table with optArgs', () => {
    const builder = new TableBuilder('users');
    const rowExpr = builder.row('age', { separator: ',' });
    expect(rowExpr).toBeInstanceOf(ExprBuilder);
    expect(rowExpr.build()).toEqual([TermType.GetField, ['age'], { separator: ',' }]);
  });

  it('should build a get term', () => {
    const builder = new TableBuilder('users');
    const get = builder.get('id123');
    expect(get.build()).toEqual([
      TermType.Get,
      [[TermType.Table, [[TermType.Database, ['default']], 'users']], 'id123']
    ]);
  });

  it('should execute get operation and return QueryResult', async () => {
    const builder = new TableBuilder<{ id: string; name: string; age: number }>('users');
    const get = builder.get('id123');

    const mockResponse: ExecutionResult<{ id: string; name: string; age: number } | null> = {
      result: { id: 'id123', name: 'Alice', age: 30 },
      stats: {
        read_count: 1,
        inserted_count: 0,
        updated_count: 0,
        deleted_count: 0,
        error_count: 0,
        duration_ms: 5
      }
    };

    const client: MockClient = { send: jest.fn().mockResolvedValue(mockResponse) };
    const result = await get.run(client as unknown as Client);
    expect(result).toBeInstanceOf(QueryResult);
    expect(result.isImmediate).toBe(true);
    expect(result.result).toEqual({ id: 'id123', name: 'Alice', age: 30 });
    expect(client.send).toHaveBeenCalled();
  });

  it('should build a filter term', () => {
    const builder = new TableBuilder('users');
    const predicate = builder.row('age').ge(21);
    const filter = builder.filter(predicate);
    expect(filter.build()).toEqual([
      TermType.Filter,
      [
        [TermType.Table, [[TermType.Database, ['default']], 'users']],
        [
          TermType.Ge,
          [
            [TermType.GetField, ['age']],
            [TermType.Datum, [21]]
          ]
        ]
      ]
    ]);
  });

  it('should return QueryResult for filter operation', async () => {
    const builder = new TableBuilder<{ name: string; age: number }>('users');
    const predicate = builder.row('age').ge(21);
    const filter = builder.filter(predicate);

    const client: MockClient = { send: jest.fn() };
    const result = await filter.run(client as unknown as Client);
    expect(result).toBeInstanceOf(QueryResult);
    expect(result.isStreaming).toBe(true);
    expect(result).toHaveProperty('toArray');
    expect(result).toHaveProperty('close');
  });

  it('table() factory should return TableBuilder', () => {
    const t = table('users');
    expect(t).toBeInstanceOf(TableBuilder);
    expect(t.build()).toEqual([TermType.Table, [[TermType.Database, ['default']], 'users']]);
  });
});
