import { EnhancedValueQuery } from '../src/query';
import { BinaryOp_Operator, NullValue } from '../src/rulo';

describe('EnhancedValueQuery', () => {
  describe('constructor', () => {
    it('should create an EnhancedValueQuery instance', () => {
      const query = { expression: { literal: { string: 'test' } } };
      const valueQuery = new EnhancedValueQuery(query);
      expect(valueQuery).toBeInstanceOf(EnhancedValueQuery);
      expect(valueQuery._query).toEqual(query);
    });
  });

  describe('eq() method', () => {
    it('should create equality comparison with string value', () => {
      const query = { expression: { literal: { string: 'name' } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.eq('John');

      expect(result).toBeInstanceOf(EnhancedValueQuery);
      expect(result._query.expression?.binary?.op).toBe(BinaryOp_Operator.EQ);
      expect(result._query.expression?.binary?.left?.subquery).toEqual(query);
      expect(result._query.expression?.binary?.right?.literal?.string).toBe('John');
    });

    it('should create equality comparison with number value', () => {
      const query = { expression: { literal: { int: '25' } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.eq(30);

      expect(result._query.expression?.binary?.op).toBe(BinaryOp_Operator.EQ);
      expect(result._query.expression?.binary?.right?.literal?.int).toBe('30');
    });

    it('should create equality comparison with float value', () => {
      const query = { expression: { literal: { float: 25.5 } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.eq(30.75);

      expect(result._query.expression?.binary?.op).toBe(BinaryOp_Operator.EQ);
      expect(result._query.expression?.binary?.right?.literal?.float).toBe(30.75);
    });

    it('should create equality comparison with boolean value', () => {
      const query = { expression: { literal: { bool: true } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.eq(false);

      expect(result._query.expression?.binary?.op).toBe(BinaryOp_Operator.EQ);
      expect(result._query.expression?.binary?.right?.literal?.bool).toBe(false);
    });

    it('should create equality comparison with null value', () => {
      const query = { expression: { literal: { string: 'field' } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.eq(null);

      expect(result._query.expression?.binary?.op).toBe(BinaryOp_Operator.EQ);
      expect(result._query.expression?.binary?.right?.literal?.null).toBe(NullValue.NULL_VALUE);
    });

    it('should create equality comparison with array value', () => {
      const query = { expression: { literal: { string: 'tags' } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.eq(['tag1', 'tag2']);

      expect(result._query.expression?.binary?.op).toBe(BinaryOp_Operator.EQ);
      expect(result._query.expression?.binary?.right?.literal?.array?.items).toHaveLength(2);
      expect(result._query.expression?.binary?.right?.literal?.array?.items[0].string).toBe('tag1');
      expect(result._query.expression?.binary?.right?.literal?.array?.items[1].string).toBe('tag2');
    });

    it('should create equality comparison with object value', () => {
      const query = { expression: { literal: { string: 'profile' } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.eq({ name: 'John', age: 30 });

      expect(result._query.expression?.binary?.op).toBe(BinaryOp_Operator.EQ);
      expect(result._query.expression?.binary?.right?.literal?.object?.fields.name.string).toBe(
        'John'
      );
      expect(result._query.expression?.binary?.right?.literal?.object?.fields.age.int).toBe('30');
    });
  });

  describe('ne() method', () => {
    it('should create not-equal comparison with string value', () => {
      const query = { expression: { literal: { string: 'status' } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.ne('inactive');

      expect(result).toBeInstanceOf(EnhancedValueQuery);
      expect(result._query.expression?.binary?.op).toBe(BinaryOp_Operator.NE);
      expect(result._query.expression?.binary?.left?.subquery).toEqual(query);
      expect(result._query.expression?.binary?.right?.literal?.string).toBe('inactive');
    });

    it('should create not-equal comparison with number value', () => {
      const query = { expression: { literal: { int: '0' } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.ne(5);

      expect(result._query.expression?.binary?.op).toBe(BinaryOp_Operator.NE);
      expect(result._query.expression?.binary?.right?.literal?.int).toBe('5');
    });

    it('should create not-equal comparison with null value', () => {
      const query = { expression: { literal: { string: 'optional_field' } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.ne(null);

      expect(result._query.expression?.binary?.op).toBe(BinaryOp_Operator.NE);
      expect(result._query.expression?.binary?.right?.literal?.null).toBe(NullValue.NULL_VALUE);
    });
  });

  describe('lt() method', () => {
    it('should create less-than comparison with number value', () => {
      const query = { expression: { literal: { int: '10' } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.lt(20);

      expect(result).toBeInstanceOf(EnhancedValueQuery);
      expect(result._query.expression?.binary?.op).toBe(BinaryOp_Operator.LT);
      expect(result._query.expression?.binary?.left?.subquery).toEqual(query);
      expect(result._query.expression?.binary?.right?.literal?.int).toBe('20');
    });

    it('should create less-than comparison with float value', () => {
      const query = { expression: { literal: { float: 10.5 } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.lt(20.75);

      expect(result._query.expression?.binary?.op).toBe(BinaryOp_Operator.LT);
      expect(result._query.expression?.binary?.right?.literal?.float).toBe(20.75);
    });

    it('should create less-than comparison with string value', () => {
      const query = { expression: { literal: { string: 'name' } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.lt('zzz');

      expect(result._query.expression?.binary?.op).toBe(BinaryOp_Operator.LT);
      expect(result._query.expression?.binary?.right?.literal?.string).toBe('zzz');
    });
  });

  describe('le() method', () => {
    it('should create less-than-or-equal comparison with number value', () => {
      const query = { expression: { literal: { int: '15' } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.le(20);

      expect(result).toBeInstanceOf(EnhancedValueQuery);
      expect(result._query.expression?.binary?.op).toBe(BinaryOp_Operator.LE);
      expect(result._query.expression?.binary?.left?.subquery).toEqual(query);
      expect(result._query.expression?.binary?.right?.literal?.int).toBe('20');
    });

    it('should create less-than-or-equal comparison with float value', () => {
      const query = { expression: { literal: { float: 15.5 } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.le(20.0);

      expect(result._query.expression?.binary?.op).toBe(BinaryOp_Operator.LE);
      expect(result._query.expression?.binary?.right?.literal?.int).toBe('20');
    });
  });

  describe('gt() method', () => {
    it('should create greater-than comparison with number value', () => {
      const query = { expression: { literal: { int: '25' } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.gt(18);

      expect(result).toBeInstanceOf(EnhancedValueQuery);
      expect(result._query.expression?.binary?.op).toBe(BinaryOp_Operator.GT);
      expect(result._query.expression?.binary?.left?.subquery).toEqual(query);
      expect(result._query.expression?.binary?.right?.literal?.int).toBe('18');
    });

    it('should create greater-than comparison with float value', () => {
      const query = { expression: { literal: { float: 25.5 } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.gt(18.25);

      expect(result._query.expression?.binary?.op).toBe(BinaryOp_Operator.GT);
      expect(result._query.expression?.binary?.right?.literal?.float).toBe(18.25);
    });

    it('should create greater-than comparison with string value', () => {
      const query = { expression: { literal: { string: 'name' } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.gt('a');

      expect(result._query.expression?.binary?.op).toBe(BinaryOp_Operator.GT);
      expect(result._query.expression?.binary?.right?.literal?.string).toBe('a');
    });
  });

  describe('ge() method', () => {
    it('should create greater-than-or-equal comparison with number value', () => {
      const query = { expression: { literal: { int: '21' } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.ge(18);

      expect(result).toBeInstanceOf(EnhancedValueQuery);
      expect(result._query.expression?.binary?.op).toBe(BinaryOp_Operator.GE);
      expect(result._query.expression?.binary?.left?.subquery).toEqual(query);
      expect(result._query.expression?.binary?.right?.literal?.int).toBe('18');
    });

    it('should create greater-than-or-equal comparison with float value', () => {
      const query = { expression: { literal: { float: 21.5 } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.ge(18.0);

      expect(result._query.expression?.binary?.op).toBe(BinaryOp_Operator.GE);
      expect(result._query.expression?.binary?.right?.literal?.int).toBe('18');
    });
  });

  describe('and() method', () => {
    it('should create AND operation with another boolean query', () => {
      const query1 = { expression: { literal: { bool: true } } };
      const query2 = { expression: { literal: { bool: false } } };

      const valueQuery1 = new EnhancedValueQuery(query1);
      const valueQuery2 = new EnhancedValueQuery(query2);

      const result = valueQuery1.and(valueQuery2);

      expect(result).toBeInstanceOf(EnhancedValueQuery);
      expect(result._query.expression?.binary?.op).toBe(BinaryOp_Operator.AND);
      expect(result._query.expression?.binary?.left?.subquery).toEqual(query1);
      expect(result._query.expression?.binary?.right?.subquery).toEqual(query2);
    });

    it('should create AND operation with comparison results', () => {
      const ageQuery = { expression: { literal: { int: '25' } } };
      const statusQuery = { expression: { literal: { string: 'active' } } };

      const ageValueQuery = new EnhancedValueQuery(ageQuery);
      const statusValueQuery = new EnhancedValueQuery(statusQuery);

      const ageComparison = ageValueQuery.gt(18);
      const statusComparison = statusValueQuery.eq('active');

      const result = ageComparison.and(statusComparison);

      expect(result._query.expression?.binary?.op).toBe(BinaryOp_Operator.AND);
      expect(result._query.expression?.binary?.left?.subquery?.expression?.binary?.op).toBe(
        BinaryOp_Operator.GT
      );
      expect(result._query.expression?.binary?.right?.subquery?.expression?.binary?.op).toBe(
        BinaryOp_Operator.EQ
      );
    });
  });

  describe('or() method', () => {
    it('should create OR operation with another boolean query', () => {
      const query1 = { expression: { literal: { bool: true } } };
      const query2 = { expression: { literal: { bool: false } } };

      const valueQuery1 = new EnhancedValueQuery(query1);
      const valueQuery2 = new EnhancedValueQuery(query2);

      const result = valueQuery1.or(valueQuery2);

      expect(result).toBeInstanceOf(EnhancedValueQuery);
      expect(result._query.expression?.binary?.op).toBe(BinaryOp_Operator.OR);
      expect(result._query.expression?.binary?.left?.subquery).toEqual(query1);
      expect(result._query.expression?.binary?.right?.subquery).toEqual(query2);
    });

    it('should create OR operation with comparison results', () => {
      const roleQuery = { expression: { literal: { string: 'user' } } };

      const roleValueQuery = new EnhancedValueQuery(roleQuery);

      const adminComparison = roleValueQuery.eq('admin');
      const moderatorComparison = roleValueQuery.eq('moderator');

      const result = adminComparison.or(moderatorComparison);

      expect(result._query.expression?.binary?.op).toBe(BinaryOp_Operator.OR);
      expect(result._query.expression?.binary?.left?.subquery?.expression?.binary?.op).toBe(
        BinaryOp_Operator.EQ
      );
      expect(result._query.expression?.binary?.right?.subquery?.expression?.binary?.op).toBe(
        BinaryOp_Operator.EQ
      );
    });
  });

  describe('match() method', () => {
    it('should create regex match with pattern', () => {
      const query = { expression: { literal: { string: 'email' } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.match('@example\\.com$');

      expect(result).toBeInstanceOf(EnhancedValueQuery);
      expect(result._query.expression?.match?.value?.subquery).toEqual(query);
      expect(result._query.expression?.match?.pattern).toBe('@example\\.com$');
      expect(result._query.expression?.match?.flags).toBe('');
    });

    it('should create regex match with pattern and flags', () => {
      const query = { expression: { literal: { string: 'name' } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.match('john', 'i');

      expect(result._query.expression?.match?.value?.subquery).toEqual(query);
      expect(result._query.expression?.match?.pattern).toBe('john');
      expect(result._query.expression?.match?.flags).toBe('i');
    });

    it('should create regex match with empty flags by default', () => {
      const query = { expression: { literal: { string: 'description' } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.match('test.*pattern');

      expect(result._query.expression?.match?.flags).toBe('');
    });
  });

  describe('complex operations', () => {
    it('should chain multiple comparison operations', () => {
      const ageQuery = { expression: { literal: { int: '25' } } };
      const ageValueQuery = new EnhancedValueQuery(ageQuery);

      const result = ageValueQuery.gt(18).and(ageValueQuery.lt(65));

      expect(result._query.expression?.binary?.op).toBe(BinaryOp_Operator.AND);
      expect(result._query.expression?.binary?.left?.subquery?.expression?.binary?.op).toBe(
        BinaryOp_Operator.GT
      );
      expect(result._query.expression?.binary?.right?.subquery?.expression?.binary?.op).toBe(
        BinaryOp_Operator.LT
      );
    });

    it('should handle complex nested logical operations', () => {
      const statusQuery = { expression: { literal: { string: 'active' } } };
      const roleQuery = { expression: { literal: { string: 'user' } } };
      const ageQuery = { expression: { literal: { int: '25' } } };

      const statusValueQuery = new EnhancedValueQuery(statusQuery);
      const roleValueQuery = new EnhancedValueQuery(roleQuery);
      const ageValueQuery = new EnhancedValueQuery(ageQuery);

      // (status == 'active') AND ((role == 'admin') OR (age > 21))
      const statusCheck = statusValueQuery.eq('active');
      const roleCheck = roleValueQuery.eq('admin');
      const ageCheck = ageValueQuery.gt(21);
      const roleOrAge = roleCheck.or(ageCheck);
      const result = statusCheck.and(roleOrAge);

      expect(result._query.expression?.binary?.op).toBe(BinaryOp_Operator.AND);
      expect(result._query.expression?.binary?.left?.subquery?.expression?.binary?.op).toBe(
        BinaryOp_Operator.EQ
      );
      expect(result._query.expression?.binary?.right?.subquery?.expression?.binary?.op).toBe(
        BinaryOp_Operator.OR
      );
    });

    it('should handle mixed comparison and pattern matching', () => {
      const emailQuery = { expression: { literal: { string: 'user@example.com' } } };
      const ageQuery = { expression: { literal: { int: '25' } } };

      const emailValueQuery = new EnhancedValueQuery(emailQuery);
      const ageValueQuery = new EnhancedValueQuery(ageQuery);

      const emailMatch = emailValueQuery.match('@example\\.com$');
      const ageCheck = ageValueQuery.ge(18);
      const result = emailMatch.and(ageCheck);

      expect(result._query.expression?.binary?.op).toBe(BinaryOp_Operator.AND);
      expect(result._query.expression?.binary?.left?.subquery?.expression?.match).toBeDefined();
      expect(result._query.expression?.binary?.right?.subquery?.expression?.binary?.op).toBe(
        BinaryOp_Operator.GE
      );
    });
  });

  describe('value conversion', () => {
    it('should handle Uint8Array values', () => {
      const query = { expression: { literal: { string: 'data' } } };
      const valueQuery = new EnhancedValueQuery(query);
      const binaryData = new Uint8Array([1, 2, 3, 4]);
      const result = valueQuery.eq(binaryData);

      expect(result._query.expression?.binary?.right?.literal?.binary).toEqual(binaryData);
    });

    it('should handle undefined values as null', () => {
      const query = { expression: { literal: { string: 'optional' } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.eq(undefined);

      expect(result._query.expression?.binary?.right?.literal?.null).toBe(NullValue.NULL_VALUE);
    });

    it('should handle nested objects', () => {
      const query = { expression: { literal: { string: 'config' } } };
      const valueQuery = new EnhancedValueQuery(query);
      const config = {
        settings: {
          theme: 'dark',
          notifications: true
        },
        preferences: ['setting1', 'setting2']
      };
      const result = valueQuery.eq(config);

      const objectFields = result._query.expression?.binary?.right?.literal?.object?.fields;
      expect(objectFields?.settings.object?.fields.theme.string).toBe('dark');
      expect(objectFields?.settings.object?.fields.notifications.bool).toBe(true);
      expect(objectFields?.preferences.array?.items).toHaveLength(2);
      expect(objectFields?.preferences.array?.items[0].string).toBe('setting1');
    });

    it('should throw error for unsupported value types', () => {
      const query = { expression: { literal: { string: 'test' } } };
      const valueQuery = new EnhancedValueQuery(query);

      expect(() => {
        valueQuery.eq(Symbol('test'));
      }).toThrow('Unsupported value type: symbol');
    });

    it('should handle function values by throwing error', () => {
      const query = { expression: { literal: { string: 'callback' } } };
      const valueQuery = new EnhancedValueQuery(query);

      expect(() => {
        valueQuery.eq(() => 'test');
      }).toThrow('Unsupported value type: function');
    });
  });

  describe('edge cases', () => {
    it('should handle empty string values', () => {
      const query = { expression: { literal: { string: 'name' } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.eq('');

      expect(result._query.expression?.binary?.right?.literal?.string).toBe('');
    });

    it('should handle zero values', () => {
      const query = { expression: { literal: { int: '5' } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.eq(0);

      expect(result._query.expression?.binary?.right?.literal?.int).toBe('0');
    });

    it('should handle negative numbers', () => {
      const query = { expression: { literal: { int: '10' } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.gt(-5);

      expect(result._query.expression?.binary?.right?.literal?.int).toBe('-5');
    });

    it('should handle very large numbers', () => {
      const query = { expression: { literal: { int: '100' } } };
      const valueQuery = new EnhancedValueQuery(query);
      const largeNumber = Number.MAX_SAFE_INTEGER;
      const result = valueQuery.lt(largeNumber);

      expect(result._query.expression?.binary?.right?.literal?.int).toBe(largeNumber.toString());
    });

    it('should handle NaN as float', () => {
      const query = { expression: { literal: { float: 5.5 } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.ne(NaN);

      expect(result._query.expression?.binary?.right?.literal?.float).toBeNaN();
    });

    it('should handle Infinity as float', () => {
      const query = { expression: { literal: { float: 100.0 } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.lt(Infinity);

      expect(result._query.expression?.binary?.right?.literal?.float).toBe(Infinity);
    });

    it('should handle empty arrays', () => {
      const query = { expression: { literal: { string: 'tags' } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.eq([]);

      expect(result._query.expression?.binary?.right?.literal?.array?.items).toHaveLength(0);
    });

    it('should handle empty objects', () => {
      const query = { expression: { literal: { string: 'metadata' } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.eq({});

      expect(
        Object.keys(result._query.expression?.binary?.right?.literal?.object?.fields || {})
      ).toHaveLength(0);
    });
  });

  describe('createExpressionQuery helper', () => {
    it('should wrap expressions in expression queries', () => {
      const query = { expression: { literal: { string: 'test' } } };
      const valueQuery = new EnhancedValueQuery(query);
      const result = valueQuery.eq('value');

      // The result should have the structure: { expression: { binary: ... } }
      expect(result._query.expression).toBeDefined();
      expect(result._query.expression?.binary).toBeDefined();
    });
  });
});
