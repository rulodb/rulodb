import { TableBuilder } from './table';
import { TermBuilder, TermOptions, TermType } from './terms';

export class DatabaseBuilder extends TermBuilder {
  constructor(name: string = 'default', optargs: TermOptions = {}) {
    super(TermType.Database, [name], optargs);
  }

  table<T extends object = Document>(name: string, options: TermOptions = {}): TableBuilder<T> {
    return new TableBuilder<T>(name, this.build(), options);
  }

  listTables<T = string>(optargs: TermOptions = {}): TermBuilder<T> {
    return new TermBuilder<T>(TermType.TableList, [this.build()], optargs);
  }

  createTable<T = string>(name: string, optargs: TermOptions = {}): TermBuilder<T> {
    return new TermBuilder<T>(TermType.TableCreate, [this.build(), name], optargs);
  }

  dropTable<T = string>(name: string, optargs: TermOptions = {}): TermBuilder<T> {
    return new TermBuilder<T>(TermType.TableDrop, [this.build(), name], optargs);
  }
}

export function db(...args: ConstructorParameters<typeof DatabaseBuilder>): DatabaseBuilder {
  return new DatabaseBuilder(...args);
}

export function listDatabases(optargs: TermOptions = {}): TermBuilder<string[]> {
  return new TermBuilder<string[]>(TermType.DatabaseList, [], optargs);
}

export function createDatabase(name: string, optargs: TermOptions = {}): TermBuilder<string> {
  return new TermBuilder<string>(TermType.DatabaseCreate, [name], optargs);
}

export function dropDatabase(name: string, optargs: TermOptions = {}): TermBuilder<string> {
  return new TermBuilder<string>(TermType.DatabaseDrop, [name], optargs);
}
