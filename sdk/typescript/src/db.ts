import { TableBuilder } from './table';
import { TermBuilder, TermOptions, TermType } from './terms';

export class DatabaseBuilder extends TermBuilder {
  constructor(name: string = 'default', optArgs: TermOptions = {}) {
    super(TermType.Database, [name], optArgs);
  }

  table<T extends object = Document>(name: string, options: TermOptions = {}): TableBuilder<T> {
    return new TableBuilder<T>(name, this.build(), options);
  }

  listTables<T = string>(optArgs: TermOptions = {}): TermBuilder<T> {
    return new TermBuilder<T>(TermType.TableList, [this.build()], optArgs);
  }

  createTable<T = string>(name: string, optArgs: TermOptions = {}): TermBuilder<T> {
    return new TermBuilder<T>(TermType.TableCreate, [this.build(), name], optArgs);
  }

  dropTable<T = string>(name: string, optArgs: TermOptions = {}): TermBuilder<T> {
    return new TermBuilder<T>(TermType.TableDrop, [this.build(), name], optArgs);
  }
}

export function db(...args: ConstructorParameters<typeof DatabaseBuilder>): DatabaseBuilder {
  return new DatabaseBuilder(...args);
}

export function listDatabases<T = string>(optArgs: TermOptions = {}): TermBuilder<T> {
  return new TermBuilder<T>(TermType.DatabaseList, [], optArgs);
}

export function createDatabase<T = string>(
  name: string,
  optArgs: TermOptions = {}
): TermBuilder<T> {
  return new TermBuilder<T>(TermType.DatabaseCreate, [name], optArgs);
}

export function dropDatabase<T = string>(name: string, optArgs: TermOptions = {}): TermBuilder<T> {
  return new TermBuilder<T>(TermType.DatabaseDrop, [name], optArgs);
}
