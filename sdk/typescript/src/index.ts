import { createDatabase, db, dropDatabase, listDatabases } from './db';
import { expr, row } from './expr';

export const r = {
  db,
  listDatabases,
  createDatabase,
  dropDatabase,
  expr,
  row
};
