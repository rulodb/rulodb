import { Client } from '../src/client';
import { QueryResult } from '../src/result';
import { ExecutionResult, MetaField, TermBuilder, TermOptions, TermType } from '../src/terms';

interface MockClient extends Pick<Client, 'send'> {
  send: jest.Mock;
}

describe('TermBuilder', () => {
  it('should build a term with correct structure', () => {
    const builder = new TermBuilder(TermType.Table, ['users']);
    expect(builder.build()).toEqual([TermType.Table, ['users']]);
  });

  it('should accept and store optArgs', () => {
    const optArgs: TermOptions = { foo: 'bar' };
    const builder = new TermBuilder(TermType.Table, ['users'], optArgs);
    expect(builder.build()).toEqual([TermType.Table, ['users'], optArgs]);
  });

  it('should return itself from debug()', () => {
    const builder = new TermBuilder(TermType.Table, ['users']);
    expect(builder.debug()).toBe(builder);
  });

  it('should return QueryResult for streaming operations like Table', async () => {
    const builder = new TermBuilder<{ name: string }>(TermType.Table, ['users']);
    const client: MockClient = { send: jest.fn() };
    const result = await builder.run(client as unknown as Client);
    expect(result).toBeInstanceOf(QueryResult);
    expect(result.isStreaming).toBe(true);
    expect(result).toHaveProperty('toArray');
    expect(result).toHaveProperty('close');
  });

  it('should execute immediately for non-streaming operations', async () => {
    const builder = new TermBuilder<Array<{ id: string; name: string }>>(TermType.Insert, [
      ['table'],
      [{ name: 'test' }]
    ]);
    const mockResponse: ExecutionResult<Array<{ id: string; name: string }>> = {
      result: [{ id: '123', name: 'test' }],
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
    const result = await builder.run(client as unknown as Client);
    expect(client.send).toHaveBeenCalledWith([TermType.Insert, [['table'], [{ name: 'test' }]]]);
    expect(result).toBeInstanceOf(QueryResult);
    expect(result.isImmediate).toBe(true);
    expect(result.result).toEqual([{ id: '123', name: 'test' }]);
  });
});

describe('TermType enum', () => {
  it('should have expected values', () => {
    expect(TermType.Table).toBe(15);
    expect(TermType.Filter).toBe(39);
    expect(TermType.Insert).toBe(56);
  });
});

describe('MetaField enum', () => {
  it('should have expected string values', () => {
    expect(MetaField.id).toBe('id');
    expect(MetaField.table).toBe('$table');
  });
});
