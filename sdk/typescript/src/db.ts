import { TableBuilder } from "./table";
import { TermBuilder, TermOptions, TermType } from "./terms";

export class DatabaseBuilder extends TermBuilder {
  constructor(name: string = "default", options: TermOptions = {}) {
    // FIXME: This should be the Database term after it is implemented.
    super(TermType.Table, [name], options);
  }

  createTable<T = string>(
    name: string,
    optargs: TermOptions = {},
  ): TermBuilder<T> {
    return new TermBuilder<T>(TermType.TableCreate, [name], optargs);
  }

  listTables<T = string>(optargs: TermOptions = {}): TermBuilder<T> {
    return new TermBuilder<T>(TermType.TableList, [], optargs);
  }

  dropTable<T = string>(
    name: string,
    optargs: TermOptions = {},
  ): TermBuilder<T> {
    return new TermBuilder<T>(TermType.TableDrop, [name], optargs);
  }

  table<T extends object = Document>(
    ...args: ConstructorParameters<typeof TableBuilder<T>>
  ): TableBuilder<T> {
    return new TableBuilder<T>(...args);
  }
}

export function db(
  ...args: ConstructorParameters<typeof DatabaseBuilder>
): DatabaseBuilder {
  return new DatabaseBuilder(...args);
}
