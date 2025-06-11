export type MandatoryField = 'id' | '$table';
export type DatabaseDocument = Record<string | MandatoryField, unknown>;

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

export type GetFieldOptions = {
  separator?: string;
};

type TermOptionsMap = {
  [TermType.Invalid]: never;
  [TermType.Datum]: never;
  [TermType.Expr]: never;
  [TermType.Eq]: never;
  [TermType.Ne]: never;
  [TermType.Lt]: never;
  [TermType.Le]: never;
  [TermType.Gt]: never;
  [TermType.Ge]: never;
  [TermType.Not]: never;
  [TermType.And]: never;
  [TermType.Or]: never;
  [TermType.Database]: never;
  [TermType.Table]: never;
  [TermType.Get]: never;
  [TermType.GetField]: GetFieldOptions;
  [TermType.Filter]: never;
  [TermType.Delete]: never;
  [TermType.Insert]: never;
  [TermType.DatabaseCreate]: never;
  [TermType.DatabaseDrop]: never;
  [TermType.DatabaseList]: never;
  [TermType.TableCreate]: never;
  [TermType.TableDrop]: never;
  [TermType.TableList]: never;
};

export type TermPrimitive = object | string | number | boolean | null;
export type TermOptions<T extends TermType = TermType> = TermOptionsMap[T];
export type TermArg = Term | TermPrimitive | TermPrimitive[];
export type TermAST = [TermType, TermArg[], TermOptions?];

export interface Term {
  toAST(): TermAST;
}
