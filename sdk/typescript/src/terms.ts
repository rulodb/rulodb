export type Join<K, P> = K extends string | number
  ? P extends string | number
    ? `${K}.${P}`
    : never
  : never;

type Prev = [never, 0, 1, 2, 3, 4, 5];

export type DeepKeys<T, D extends number = 5> = [D] extends [0]
  ? never
  : {
      [K in keyof T & (string | number)]: T[K] extends object
        ? K | Join<K, DeepKeys<T[K], Prev[D]>>
        : K;
    }[keyof T & (string | number)];

export type DeepGet<T, P extends string, D extends number = 5> = [D] extends [0]
  ? unknown
  : P extends keyof T
    ? T[P]
    : P extends `${infer K}.${infer Rest}`
      ? K extends keyof T
        ? T[K] extends object
          ? DeepGet<T[K], Rest, Prev[D]>
          : unknown
        : unknown
      : unknown;

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
  Database = 14,
  Table = 15,
  Get = 16,
  GetField = 31,
  Filter = 39,
  Delete = 54,
  Insert = 56,
  DatabaseCreate = 57,
  DatabaseDrop = 58,
  DatabaseList = 59,
  TableCreate = 60,
  TableDrop = 61,
  TableList = 62
}

export enum MetaField {
  id = 'id',
  table = '$table'
}

export type MandatoryField = 'id' | '$table';

export type Document = Record<string | MandatoryField, unknown>;

export type TermOptions = Record<string, unknown>;

export type TermArgs = Partial<Array<unknown>>;

export type Term = [number, TermArgs, TermOptions] | [number, TermArgs];

export type QueryResponse<T = Document> = {
  result: T | T[] | null;
  explanation: string;
  stats: {
    read_count: number;
    inserted_count: number;
    deleted_count: number;
    error_count: number;
    duration_ms: number;
  };
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
    console.dir(this.build(), { depth: null });
    return this;
  }

  async run(client: { send(query: Term): Promise<QueryResponse<T>> }) {
    return await client.send(this.build());
  }
}
