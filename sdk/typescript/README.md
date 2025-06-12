# RuloDB TypeScript SDK

A strongly-typed TypeScript client for RuloDB with a fluent query builder API similar to RethinkDB.

## Installation

```shell
npm install @rulodb/rulodb
```

## Quick Start

```typescript
import { RuloDB, r } from '@rulodb/rulodb';

// Create and connect to RuloDB
const client = new RuloDB({
  host: 'localhost',
  port: 6090,
  timeout: 30000
});

await client.connect();

// Insert a document
await r
  .table('users')
  .insert({
    name: 'John Doe',
    email: 'john@example.com',
    age: 30
  })
  .run(client);

// Query documents
const users = await r.table('users')
    .filter({ active: true })
    .orderBy('name')
    .limit(10)
    .run(client);

// Collect cursor and display users
console.table(await users.toArray());

await client.disconnect();
```

## Contributing

See [CONTRIBUTING.md](https://github.com/rulodb/rulodb/blob/main/CONTRIBUTING.md) for development setup and contribution guidelines.

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.
