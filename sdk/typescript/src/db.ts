import { TableBuilder } from './table';
import { TermBuilder, TermOptions, TermType } from './terms';

export class DatabaseBuilder extends TermBuilder {
  constructor(name: string = 'default', optArgs: TermOptions = {}) {
    super(TermType.Database, [name], optArgs);
  }

  table<T extends object = Record<string, unknown>>(
    name: string,
    options: TermOptions = {}
  ): TableBuilder<T> {
    return new TableBuilder<T>(name, this.build(), options);
  }

  listTables(optArgs: TermOptions = {}): TermBuilder<string[]> {
    return new TermBuilder<string[]>(TermType.TableList, [this.build()], optArgs);
  }

  createTable(name: string, optArgs: TermOptions = {}): TermBuilder<string> {
    return new TermBuilder<string>(TermType.TableCreate, [this.build(), name], optArgs);
  }

  dropTable(name: string, optArgs: TermOptions = {}): TermBuilder<string> {
    return new TermBuilder<string>(TermType.TableDrop, [this.build(), name], optArgs);
  }
}

export function db(...args: ConstructorParameters<typeof DatabaseBuilder>): DatabaseBuilder {
  return new DatabaseBuilder(...args);
}

export function listDatabases(optArgs: TermOptions = {}): TermBuilder<string[]> {
  return new TermBuilder<string[]>(TermType.DatabaseList, [], optArgs);
}

export function createDatabase(name: string, optArgs: TermOptions = {}): TermBuilder<string> {
  return new TermBuilder<string>(TermType.DatabaseCreate, [name], optArgs);
}

export function dropDatabase(name: string, optArgs: TermOptions = {}): TermBuilder<string> {
  return new TermBuilder<string>(TermType.DatabaseDrop, [name], optArgs);
}
