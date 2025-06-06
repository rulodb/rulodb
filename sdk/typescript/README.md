# RuloDB TypeScript SDK

TypeScript SDK and client reference implementation for [RuloDB](https://github.com/rulodb/rulodb).

## Installation

```shell
npm install @rulodb/rulodb
```

## Usage

```ts
import { r } from '@rulodb/rulodb';
import { Client } from './client';

const client = new Client();
await client.connect('127.0.0.1', 6969);

const queryResponse = await r.table('default').insert({ name: 'Thor', power: 100 }).run(client);
console.table(queryResponse.result);

client.close();
```

## Running Tests

```shell
npm run test
```
