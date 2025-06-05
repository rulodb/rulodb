import { MetaField, QueryResponse, TermBuilder, TermOptions, TermType } from '../src/terms';

describe('TermBuilder', () => {
  it('should build a term with correct structure', () => {
    const builder = new TermBuilder(TermType.Table, ['users']);
    expect(builder.build()).toEqual([TermType.Table, ['users'], {}]);
  });

  it('should accept and store optargs', () => {
    const optargs: TermOptions = { foo: 'bar' };
    const builder = new TermBuilder(TermType.Table, ['users'], optargs);
    expect(builder.build()).toEqual([TermType.Table, ['users'], optargs]);
  });

  it('should return itself from debug()', () => {
    const builder = new TermBuilder(TermType.Table, ['users']);
    expect(builder.debug()).toBe(builder);
  });

  it('should call client.send with built term in run()', async () => {
    const builder = new TermBuilder(TermType.Table, ['users']);
    const mockResponse: QueryResponse = {
      result: null,
      explanation: '',
      stats: {
        read_count: 0,
        inserted_count: 0,
        deleted_count: 0,
        error_count: 0,
        duration_ms: 0
      }
    };
    const client = { send: jest.fn().mockResolvedValue(mockResponse) };
    const result = await builder.run(client);
    expect(client.send).toHaveBeenCalledWith([TermType.Table, ['users'], {}]);
    expect(result).toBe(mockResponse);
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
