import { Client, connect } from './client';
import { Cursor } from './cursor';
import { createDatabase, db, dropDatabase, listDatabases } from './db';
import { expr, row } from './expr';
import { QueryResult } from './result';

export const r = {
  connect,
  db,
  listDatabases,
  createDatabase,
  dropDatabase,
  expr,
  row
};

export { Client, Cursor, QueryResult };
