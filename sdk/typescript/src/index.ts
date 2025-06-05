import { createDatabase, db, dropDatabase, listDatabases } from './db';
import { expr } from './expr';

export const r = {
  db,
  listDatabases,
  createDatabase,
  dropDatabase,
  expr
};
