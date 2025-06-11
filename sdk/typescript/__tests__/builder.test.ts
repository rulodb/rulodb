import { createTerm } from '../src/builder';
import { TermType } from '../src/types';

describe('Term Builder', () => {
  it('should create a term with no options', () => {
    const term = createTerm(TermType.Database, ['test']);
    expect(term.toAST()).toEqual([TermType.Database, ['test']]);
  });

  it('should create a term with options', () => {
    const term = createTerm(TermType.GetField, ['test'], { separator: ',' });
    expect(term.toAST()).toEqual([TermType.GetField, ['test'], { separator: ',' }]);
  });
});
