# RuloDB TypeScript SDK

TypeScript SDK and client reference implementation for [RuloDB](https://github.com/rulodb/rulodb).

## Installation

```shell
npm install @rulodb/rulodb
```

## Flexible Builder System

The RuloDB TypeScript SDK features a flexible builder that supports both **direct method calls** and **expression-based calls**:

### Direct Method Calls

Use comparison operators directly on the `r` object:

```ts
import { r } from '@rulodb/rulodb';

// Direct comparisons
const equal = r.eq(1, 1);
const notEqual = r.ne('hello', 'world');
const lessThan = r.lt(5, 10);
const greaterThan = r.gt(15, 10);

// Logical operations
const andCondition = r.and(equal, lessThan);
const orCondition = r.or(notEqual, greaterThan);
const notCondition = r.not(equal);
```

### Expression-Based Calls (Fluent API)

Build expressions by chaining methods on field accessors:

```ts
import { r, Document } from '@rulodb/rulodb';

// Define your document type
interface User extends Document {
  id: string;
  name: string;
  age: number;
  isActive: boolean;
}

// Fluent field access and comparisons
const ageCheck = r.row<User>('age').gt(18);
const nameCheck = r.row<User>('name').eq('John');
const activeCheck = r.row<User>('isActive').eq(true);

// Chain conditions
const complexQuery = r.and(
  r.and(ageCheck, nameCheck),
  activeCheck
);
```

### Mixed Usage

Combine both approaches in the same expression:

```ts
const mixedQuery = r.and(
  r.eq(1, 1),  // Direct call
  r.row<User>('age').ge(21)  // Fluent call
);
```

### Expression Wrapping

Use `r.expr()` to wrap complex expressions:

```ts
const wrappedExpression = r.expr([complexQuery]);
const negatedExpression = r.not(wrappedExpression);
```

## Type Safety

The flexible builder provides full TypeScript type safety:

```ts
interface Product extends Document {
  id: string;
  title: string;
  price: number;
  category: string;
}

// ✓ This works - 'price' exists on Product
const priceCheck = r.row<Product>('price').gt(100);

// ✗ This would cause a TypeScript error
// const invalidField = r.row<Product>('invalidField');
```

## Complete Example

```ts
import { r, Document } from '@rulodb/rulodb';

interface User extends Document {
  id: string;
  name: string;
  age: number;
  email: string;
  isActive: boolean;
}

// Authentication check
const authCheck = r.and(
  r.row<User>('email').eq('user@example.com'),
  r.row<User>('isActive').eq(true)
);

// Age range query
const ageRange = r.and(
  r.row<User>('age').ge(18),
  r.row<User>('age').le(65)
);

// Complex query with mixed approaches
const userQuery = r.or(
  r.eq('admin', 'admin'),  // Direct call
  r.and(authCheck, ageRange)  // Fluent calls
);

console.log(userQuery.toAST());
```

## Available Methods

### Direct Methods
- `r.eq(left, right)` - Equality comparison
- `r.ne(left, right)` - Not equal comparison
- `r.lt(left, right)` - Less than comparison
- `r.le(left, right)` - Less than or equal comparison
- `r.gt(left, right)` - Greater than comparison
- `r.ge(left, right)` - Greater than or equal comparison
- `r.and(left, right)` - Logical AND
- `r.or(left, right)` - Logical OR
- `r.not(term)` - Logical NOT

### Fluent Methods
- `r.row<T>(field)` - Access document field
- `.eq(value)` - Equality comparison
- `.ne(value)` - Not equal comparison
- `.lt(value)` - Less than comparison
- `.le(value)` - Less than or equal comparison
- `.gt(value)` - Greater than comparison
- `.ge(value)` - Greater than or equal comparison
- `.and(other)` - Logical AND
- `.or(other)` - Logical OR
- `.not()` - Logical NOT

### Expression Methods
- `r.expr(args)` - Wrap arguments in an expression

## Running Tests

```shell
npm run test
```

## Examples

See the `examples/` directory for comprehensive usage examples and demos.
