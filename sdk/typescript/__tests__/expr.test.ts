import { Client, DatabaseDocument, r, TermType } from '../src/index';

// Mock the client
jest.mock('../src/client');

describe('Expression Operations', () => {
  let mockClient: jest.Mocked<Client>;

  beforeEach(() => {
    mockClient = {
      send: jest.fn(),
      close: jest.fn()
    } as any;
  });

  describe('Row field access', () => {
    it('should create row field access with string field', () => {
      const rowField = r.row('name');
      expect(rowField).toBeDefined();
      expect(rowField._context).toBe('expr');

      const ast = rowField.toAST();
      expect(ast[0]).toBe(TermType.GetField);
      expect(ast[1]).toEqual(['name']);
    });

    it('should create row field access with nested field path', () => {
      interface User extends DatabaseDocument {
        id: string;
        profile: {
          name: string;
          contact: {
            email: string;
          };
        };
      }

      const nestedField = r.row<User>('profile.contact.email');
      expect(nestedField).toBeDefined();

      const ast = nestedField.toAST();
      expect(ast[0]).toBe(TermType.GetField);
      expect(ast[1]).toEqual(['profile.contact.email']);
    });

    it('should create row field access with options', () => {
      const rowField = r.row('tags', { separator: '.' });
      expect(rowField).toBeDefined();

      const ast = rowField.toAST();
      expect(ast[0]).toBe(TermType.GetField);
      expect(ast[1]).toEqual(['tags']);
      expect(ast[2]).toEqual({ separator: '.' });
    });

    it('should support getField method on expressions', () => {
      const baseExpr = r.row('user');
      const fieldExpr = baseExpr.getField('profile');

      expect(fieldExpr).toBeDefined();
      expect(fieldExpr._context).toBe('expr');

      const ast = fieldExpr.toAST();
      expect(ast[0]).toBe(TermType.GetField);
    });
  });

  describe('Comparison operations', () => {
    describe('Equality operations', () => {
      it('should create eq expression from root', () => {
        const eqExpr = r.eq(1, 1);
        expect(eqExpr).toBeDefined();
        expect(eqExpr._context).toBe('expr');

        const ast = eqExpr.toAST();
        expect(ast[0]).toBe(TermType.Eq);
        expect(ast[1]).toEqual([1, 1]);
      });

      it('should create eq expression from field', () => {
        const field = r.row('age');
        const eqExpr = field.eq(25);

        expect(eqExpr).toBeDefined();
        expect(eqExpr._context).toBe('expr');

        const ast = eqExpr.toAST();
        expect(ast[0]).toBe(TermType.Eq);
        expect(ast[1]).toHaveLength(2);
      });

      it('should handle complex equality comparisons', () => {
        const leftField = r.row('user.id');
        const rightField = r.row('target.id');
        const eqExpr = r.eq(leftField, rightField);

        expect(eqExpr).toBeDefined();

        const ast = eqExpr.toAST();
        expect(ast[0]).toBe(TermType.Eq);
      });

      it('should create ne expression', () => {
        const neExpr = r.ne('admin', r.row('role'));
        expect(neExpr).toBeDefined();

        const ast = neExpr.toAST();
        expect(ast[0]).toBe(TermType.Ne);
      });

      it('should create ne expression from field', () => {
        const field = r.row('status');
        const neExpr = field.ne('deleted');

        expect(neExpr).toBeDefined();

        const ast = neExpr.toAST();
        expect(ast[0]).toBe(TermType.Ne);
      });
    });

    describe('Ordering operations', () => {
      it('should create lt expression', () => {
        const ltExpr = r.lt(5, 10);
        expect(ltExpr).toBeDefined();

        const ast = ltExpr.toAST();
        expect(ast[0]).toBe(TermType.Lt);
        expect(ast[1]).toEqual([5, 10]);
      });

      it('should create lt expression from field', () => {
        const ageField = r.row('age');
        const ltExpr = ageField.lt(18);

        expect(ltExpr).toBeDefined();

        const ast = ltExpr.toAST();
        expect(ast[0]).toBe(TermType.Lt);
      });

      it('should create le expression', () => {
        const leExpr = r.le(10, 10);
        expect(leExpr).toBeDefined();

        const ast = leExpr.toAST();
        expect(ast[0]).toBe(TermType.Le);
      });

      it('should create le expression from field', () => {
        const scoreField = r.row('score');
        const leExpr = scoreField.le(100);

        expect(leExpr).toBeDefined();

        const ast = leExpr.toAST();
        expect(ast[0]).toBe(TermType.Le);
      });

      it('should create gt expression', () => {
        const gtExpr = r.gt(15, 10);
        expect(gtExpr).toBeDefined();

        const ast = gtExpr.toAST();
        expect(ast[0]).toBe(TermType.Gt);
      });

      it('should create gt expression from field', () => {
        const priceField = r.row('price');
        const gtExpr = priceField.gt(0);

        expect(gtExpr).toBeDefined();

        const ast = gtExpr.toAST();
        expect(ast[0]).toBe(TermType.Gt);
      });

      it('should create ge expression', () => {
        const geExpr = r.ge(20, 18);
        expect(geExpr).toBeDefined();

        const ast = geExpr.toAST();
        expect(ast[0]).toBe(TermType.Ge);
      });

      it('should create ge expression from field', () => {
        const ratingField = r.row('rating');
        const geExpr = ratingField.ge(4.0);

        expect(geExpr).toBeDefined();

        const ast = geExpr.toAST();
        expect(ast[0]).toBe(TermType.Ge);
      });
    });

    describe('Complex comparison scenarios', () => {
      it('should handle mixed type comparisons', () => {
        const stringComparison = r.eq('string', r.row('text_field'));
        const numberComparison = r.gt(42, r.row('number_field'));
        const booleanComparison = r.eq(true, r.row('flag'));

        expect(stringComparison).toBeDefined();
        expect(numberComparison).toBeDefined();
        expect(booleanComparison).toBeDefined();
      });

      it('should chain multiple comparisons', () => {
        const ageField = r.row('age');
        const chainedExpr = ageField.gt(18).and(ageField.lt(65));

        expect(chainedExpr).toBeDefined();

        const ast = chainedExpr.toAST();
        expect(ast[0]).toBe(TermType.And);
      });
    });
  });

  describe('Logical operations', () => {
    describe('AND operations', () => {
      it('should create and expression from root', () => {
        const leftExpr = r.gt(r.row('age'), 18);
        const rightExpr = r.eq(r.row('status'), 'active');
        const andExpr = r.and(leftExpr, rightExpr);

        expect(andExpr).toBeDefined();
        expect(andExpr._context).toBe('expr');

        const ast = andExpr.toAST();
        expect(ast[0]).toBe(TermType.And);
        expect(ast[1]).toHaveLength(2);
      });

      it('should create and expression from field', () => {
        const baseExpr = r.row('verified').eq(true);
        const andExpr = baseExpr.and(r.row('active').eq(true));

        expect(andExpr).toBeDefined();

        const ast = andExpr.toAST();
        expect(ast[0]).toBe(TermType.And);
      });

      it('should chain multiple and operations', () => {
        const expr1 = r.row('age').gt(18);
        const expr2 = r.row('status').eq('active');
        const expr3 = r.row('verified').eq(true);

        const chainedAnd = expr1.and(expr2).and(expr3);

        expect(chainedAnd).toBeDefined();

        const ast = chainedAnd.toAST();
        expect(ast[0]).toBe(TermType.And);
      });
    });

    describe('OR operations', () => {
      it('should create or expression from root', () => {
        const leftExpr = r.eq(r.row('role'), 'admin');
        const rightExpr = r.eq(r.row('role'), 'moderator');
        const orExpr = r.or(leftExpr, rightExpr);

        expect(orExpr).toBeDefined();

        const ast = orExpr.toAST();
        expect(ast[0]).toBe(TermType.Or);
      });

      it('should create or expression from field', () => {
        const statusExpr = r.row('status').eq('pending');
        const orExpr = statusExpr.or(r.row('status').eq('review'));

        expect(orExpr).toBeDefined();

        const ast = orExpr.toAST();
        expect(ast[0]).toBe(TermType.Or);
      });

      it('should chain multiple or operations', () => {
        const role1 = r.row('role').eq('user');
        const role2 = r.row('role').eq('admin');
        const role3 = r.row('role').eq('moderator');

        const chainedOr = role1.or(role2).or(role3);

        expect(chainedOr).toBeDefined();

        const ast = chainedOr.toAST();
        expect(ast[0]).toBe(TermType.Or);
      });
    });

    describe('NOT operations', () => {
      it('should create not expression from root', () => {
        const baseExpr = r.eq(r.row('deleted'), true);
        const notExpr = r.not(baseExpr);

        expect(notExpr).toBeDefined();

        const ast = notExpr.toAST();
        expect(ast[0]).toBe(TermType.Not);
        expect(ast[1]).toHaveLength(1);
      });

      it('should create not expression from field', () => {
        const activeExpr = r.row('active').eq(true);
        const notExpr = activeExpr.not();

        expect(notExpr).toBeDefined();

        const ast = notExpr.toAST();
        expect(ast[0]).toBe(TermType.Not);
      });

      it('should handle double negation', () => {
        const baseExpr = r.row('valid').eq(true);
        const doubleNot = baseExpr.not().not();

        expect(doubleNot).toBeDefined();

        const ast = doubleNot.toAST();
        expect(ast[0]).toBe(TermType.Not);

        // The inner not should also be a Not term
        const innerArg = ast[1][0];
        if (typeof innerArg === 'object' && innerArg && 'toAST' in innerArg) {
          const innerAst = (innerArg as any).toAST();
          expect(innerAst[0]).toBe(TermType.Not);
        }
      });
    });

    describe('Complex logical combinations', () => {
      it('should handle nested logical expressions', () => {
        const ageCheck = r.and(r.row('age').ge(18), r.row('age').le(65));
        const statusCheck = r.or(r.row('status').eq('active'), r.row('status').eq('pending'));
        const combinedExpr = r.and(ageCheck, statusCheck);

        expect(combinedExpr).toBeDefined();

        const ast = combinedExpr.toAST();
        expect(ast[0]).toBe(TermType.And);
      });

      it('should handle precedence with mixed operations', () => {
        // (age > 18 AND verified = true) OR (role = 'admin')
        const adultVerified = r.and(r.row('age').gt(18), r.row('verified').eq(true));
        const isAdmin = r.row('role').eq('admin');
        const finalExpr = r.or(adultVerified, isAdmin);

        expect(finalExpr).toBeDefined();

        const ast = finalExpr.toAST();
        expect(ast[0]).toBe(TermType.Or);
      });

      it('should handle complex boolean algebra', () => {
        // NOT (status = 'deleted' OR status = 'banned')
        const deletedOrBanned = r.or(r.row('status').eq('deleted'), r.row('status').eq('banned'));
        const notDeletedOrBanned = r.not(deletedOrBanned);

        expect(notDeletedOrBanned).toBeDefined();

        const ast = notDeletedOrBanned.toAST();
        expect(ast[0]).toBe(TermType.Not);
      });
    });
  });

  describe('Expression execution', () => {
    it('should execute simple expression', async () => {
      const expectedResult = true;
      mockClient.send.mockResolvedValue(expectedResult);

      const expr = r.eq(1, 1);
      const result = await expr.run(mockClient);

      expect(result).toBe(expectedResult);
      expect(mockClient.send).toHaveBeenCalledWith(expr.toAST());
    });

    it('should execute field expression', async () => {
      const expectedResult = 'John Doe';
      mockClient.send.mockResolvedValue(expectedResult);

      const expr = r.row('name');
      const result = await expr.run(mockClient);

      expect(result).toBe(expectedResult);
    });

    it('should execute complex logical expression', async () => {
      const expectedResult = false;
      mockClient.send.mockResolvedValue(expectedResult);

      const complexExpr = r.and(r.row('age').gt(18), r.row('verified').eq(true));
      const result = await complexExpr.run(mockClient);

      expect(result).toBe(expectedResult);
    });

    it('should execute expression with generic type', async () => {
      interface ResultType {
        matches: boolean;
        score: number;
      }

      const expectedResult: ResultType = { matches: true, score: 0.95 };
      mockClient.send.mockResolvedValue(expectedResult);

      const expr = r.row('search_result');
      const result = await expr.run<ResultType>(mockClient);

      expect(result).toEqual(expectedResult);
    });

    it('should handle execution errors', async () => {
      const error = new Error('Expression evaluation failed');
      mockClient.send.mockRejectedValue(error);

      const expr = r.row('nonexistent_field');

      await expect(expr.run(mockClient)).rejects.toThrow('Expression evaluation failed');
    });
  });

  describe('Expression debugging', () => {
    it('should debug simple expression', () => {
      const consoleSpy = jest.spyOn(console, 'dir').mockImplementation();

      const expr = r.eq(1, 2);
      const debugResult = expr.debug();

      expect(consoleSpy).toHaveBeenCalledWith(expr.toAST(), { depth: null });
      expect(debugResult).toBe(expr);

      consoleSpy.mockRestore();
    });

    it('should debug complex expression', () => {
      const consoleSpy = jest.spyOn(console, 'dir').mockImplementation();

      const complexExpr = r.and(
        r.row('age').gt(18),
        r.or(r.row('role').eq('admin'), r.row('verified').eq(true))
      );

      const debugResult = complexExpr.debug();

      expect(consoleSpy).toHaveBeenCalledWith(complexExpr.toAST(), { depth: null });
      expect(debugResult).toBe(complexExpr);

      consoleSpy.mockRestore();
    });

    it('should debug field expression', () => {
      const consoleSpy = jest.spyOn(console, 'dir').mockImplementation();

      const fieldExpr = r.row('user.profile.name');
      const debugResult = fieldExpr.debug();

      expect(consoleSpy).toHaveBeenCalledWith(fieldExpr.toAST(), { depth: null });
      expect(debugResult).toBe(fieldExpr);

      consoleSpy.mockRestore();
    });
  });

  describe('Expression utilities', () => {
    it('should create expr from array of terms', () => {
      const terms = [r.row('field1').eq('value1'), r.row('field2').gt(100)];

      const exprTerm = r.expr(terms);
      expect(exprTerm).toBeDefined();
      expect(exprTerm._context).toBe('expr');

      const ast = exprTerm.toAST();
      expect(ast[0]).toBe(TermType.Expr);
      expect(ast[1]).toHaveLength(2);
    });

    it('should handle empty expr array', () => {
      const emptyExpr = r.expr([]);
      expect(emptyExpr).toBeDefined();

      const ast = emptyExpr.toAST();
      expect(ast[0]).toBe(TermType.Expr);
      expect(ast[1]).toEqual([]);
    });
  });

  describe('Type safety and inference', () => {
    it('should maintain type safety with typed row access', () => {
      interface User extends DatabaseDocument {
        id: string;
        name: string;
        age: number;
        profile: {
          email: string;
          verified: boolean;
        };
      }

      const nameField = r.row<User>('name');
      const ageField = r.row<User>('age');
      const emailField = r.row<User>('profile.email');

      expect(nameField).toBeDefined();
      expect(ageField).toBeDefined();
      expect(emailField).toBeDefined();

      // These should create valid expressions
      const nameCheck = nameField.eq('John');
      const ageCheck = ageField.gt(18);
      const emailCheck = emailField.ne('');

      expect(nameCheck._context).toBe('expr');
      expect(ageCheck._context).toBe('expr');
      expect(emailCheck._context).toBe('expr');
    });

    it('should support method chaining with proper context', () => {
      const expr = r
        .row('status')
        .eq('active')
        .and(r.row('verified').eq(true))
        .or(r.row('role').eq('admin'))
        .not();

      expect(expr).toBeDefined();
      expect(expr._context).toBe('expr');

      const ast = expr.toAST();
      expect(ast[0]).toBe(TermType.Not);
    });
  });

  describe('Edge cases and error scenarios', () => {
    it('should handle null and undefined values in comparisons', () => {
      const nullComparison = r.eq(null, r.row('nullable_field'));
      const undefinedComparison = r.ne(null, r.row('optional_field'));

      expect(nullComparison).toBeDefined();
      expect(undefinedComparison).toBeDefined();

      const nullAst = nullComparison.toAST();
      const undefinedAst = undefinedComparison.toAST();

      expect(nullAst[1]).toContain(null);
      expect(undefinedAst[1]).toContain(null);
    });

    it('should handle deeply nested field paths', () => {
      const deepField = r.row('level1.level2.level3.level4.value');
      expect(deepField).toBeDefined();

      const ast = deepField.toAST();
      expect(ast[0]).toBe(TermType.GetField);
      expect(ast[1]).toEqual(['level1.level2.level3.level4.value']);
    });

    it('should handle special characters in field names', () => {
      const specialField = r.row('field-with-dashes_and_underscores.123');
      expect(specialField).toBeDefined();

      const ast = specialField.toAST();
      expect(ast[1]).toEqual(['field-with-dashes_and_underscores.123']);
    });

    it('should handle circular reference detection in complex expressions', () => {
      // Create a complex expression that could potentially cause issues
      const baseExpr = r.row('base').eq('value');
      let complexExpr = baseExpr;

      // Chain many operations
      for (let i = 0; i < 100; i++) {
        complexExpr = complexExpr.and(r.row(`field${i}`).gt(i));
      }

      expect(complexExpr).toBeDefined();
      expect(complexExpr._context).toBe('expr');
    });
  });

  describe('Expression AST structure validation', () => {
    it('should generate valid AST for all comparison operations', () => {
      const operations = [
        { op: r.eq(1, 2), type: TermType.Eq },
        { op: r.ne(1, 2), type: TermType.Ne },
        { op: r.lt(1, 2), type: TermType.Lt },
        { op: r.le(1, 2), type: TermType.Le },
        { op: r.gt(1, 2), type: TermType.Gt },
        { op: r.ge(1, 2), type: TermType.Ge }
      ];

      operations.forEach(({ op, type }) => {
        const ast = op.toAST();
        expect(ast).toHaveLength(2);
        expect(ast[0]).toBe(type);
        expect(Array.isArray(ast[1])).toBe(true);
        expect(ast[1]).toHaveLength(2);
      });
    });

    it('should generate valid AST for logical operations', () => {
      const expr1 = r.row('a').eq(1);
      const expr2 = r.row('b').eq(2);

      const operations = [
        { op: r.and(expr1, expr2), type: TermType.And },
        { op: r.or(expr1, expr2), type: TermType.Or },
        { op: r.not(expr1), type: TermType.Not }
      ];

      operations.forEach(({ op, type }) => {
        const ast = op.toAST();
        expect(ast[0]).toBe(type);
        expect(Array.isArray(ast[1])).toBe(true);

        if (type === TermType.Not) {
          expect(ast[1]).toHaveLength(1);
        } else {
          expect(ast[1]).toHaveLength(2);
        }
      });
    });

    it('should handle nested AST structures correctly', () => {
      const nestedExpr = r.and(r.or(r.row('a').eq(1), r.row('b').eq(2)), r.not(r.row('c').eq(3)));

      const ast = nestedExpr.toAST();
      expect(ast[0]).toBe(TermType.And);
      expect(ast[1]).toHaveLength(2);

      // Check that nested terms are resolved to their AST forms
      const leftArg = ast[1][0];
      const rightArg = ast[1][1];

      // Arguments should be resolved to their AST form
      if (Array.isArray(leftArg)) {
        expect(leftArg[0]).toBe(TermType.Or);
      } else {
        expect(leftArg).toHaveProperty('toAST');
        if (typeof leftArg === 'object' && leftArg && 'toAST' in leftArg) {
          const leftAst = (leftArg as any).toAST();
          expect(leftAst[0]).toBe(TermType.Or);
        }
      }

      if (Array.isArray(rightArg)) {
        expect(rightArg[0]).toBe(TermType.Not);
      } else {
        expect(rightArg).toHaveProperty('toAST');
        if (typeof rightArg === 'object' && rightArg && 'toAST' in rightArg) {
          const rightAst = (rightArg as any).toAST();
          expect(rightAst[0]).toBe(TermType.Not);
        }
      }
    });
  });
});
