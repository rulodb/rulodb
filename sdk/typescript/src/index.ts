import { connect } from './client';
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

export { QueryResult };
