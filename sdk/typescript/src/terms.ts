export type Join<K, P> = K extends string | number
  ? P extends string | number
    ? `${K}.${P}`
    : never
  : never;

export type DeepKeys<T> = {
  [K in keyof T & (string | number)]: T[K] extends object
    ? K | Join<K, DeepKeys<T[K]>>
    : K;
}[keyof T & (string | number)];

// @ts-expect-error the type is intentionally recursive
export type FieldAccessor<T> = <K extends DeepKeys<T>>(path: K) => TermBuilder;

export enum TermType {
  Invalid = 0,
  Datum = 1,
  Expr = 2,
  Eq = 17,
  Ne = 18,
  Lt = 19,
  Le = 20,
  Gt = 21,
  Ge = 22,
  Not = 23,
  And = 66,
  Or = 67,
  Table = 15,
  Get = 16,
  GetField = 31,
  Filter = 39,
  Delete = 54,
  Insert = 56,
  TableCreate = 60,
  TableDrop = 61,
  TableList = 62,
}

export enum MetaField {
  id = "id",
  table = "$table",
}

export type MandatoryField = "id" | "$table";

export type Document = Record<string | MandatoryField, unknown>;

export type TermOptions = Record<string, unknown>;

export type TermArgs = Partial<Array<unknown>>;

export type Term = [number, TermArgs, TermOptions];

export type QueryResponse<T = Document> = {
  result: T | T[] | null;
  explanation: string;
  stats: string;
};

export class TermBuilder<T = unknown> {
  protected term: Term;

  constructor(type: TermType, args: TermArgs, optargs: TermOptions = {}) {
    this.term = [type, args, optargs];
  }

  build(): Term {
    return this.term;
  }

  debug(): this {
    return this;
  }

  async run(client: { send(query: Term): Promise<QueryResponse<T>> }) {
    return await client.send(this.build());
  }
}
