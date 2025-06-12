use crate::ast::*;

/// Cost constants for different operations
pub const TABLE_SCAN_COST: f64 = 1.0;
pub const FILTER_COST: f64 = 0.1;
pub const GET_COST: f64 = 0.5;

/// Represents a node in the query execution plan
#[derive(Debug, Clone)]
pub enum PlanNode {
    /// Return a constant value
    Constant {
        value: Datum,
        cost: f64,
    },

    // Database operations
    CreateDatabase {
        name: String,
        cost: f64,
    },
    DropDatabase {
        name: String,
        cost: f64,
    },
    ListDatabases {
        cursor: Option<Cursor>,
        cost: f64,
    },

    // Table operations
    TableScan {
        table_ref: TableRef,
        cursor: Option<Cursor>,
        filter: Option<Expression>,
        cost: f64,
        estimated_rows: f64,
    },
    CreateTable {
        table_ref: TableRef,
        cost: f64,
    },
    DropTable {
        table_ref: TableRef,
        cost: f64,
    },
    ListTables {
        database_ref: DatabaseRef,
        cursor: Option<Cursor>,
        cost: f64,
    },

    // Document operations
    Get {
        table_ref: TableRef,
        key: String,
        cost: f64,
    },
    GetAll {
        table_ref: TableRef,
        keys: Vec<String>,
        cursor: Option<Cursor>,
        cost: f64,
    },

    // Mutation operations
    Insert {
        table_ref: TableRef,
        documents: Vec<DatumObject>,
        cost: f64,
    },
    Update {
        source: Box<PlanNode>,
        patch: DatumObject,
        cost: f64,
    },
    Delete {
        source: Box<PlanNode>,
        cost: f64,
    },

    // Query operations
    Filter {
        source: Box<PlanNode>,
        predicate: Expression,
        cost: f64,
        selectivity: f64,
    },
    OrderBy {
        source: Box<PlanNode>,
        fields: Vec<OrderByField>,
        cost: f64,
    },
    Limit {
        source: Box<PlanNode>,
        count: u32,
        cost: f64,
    },
    Skip {
        source: Box<PlanNode>,
        count: u32,
        cost: f64,
    },
    Count {
        source: Box<PlanNode>,
        cost: f64,
    },

    // Subquery
    Subquery {
        query: Box<PlanNode>,
        cost: f64,
    },
}

impl PlanNode {
    /// Get the cost of this plan node
    pub fn cost(&self) -> f64 {
        match self {
            PlanNode::Constant { cost, .. } => *cost,
            PlanNode::CreateDatabase { cost, .. } => *cost,
            PlanNode::DropDatabase { cost, .. } => *cost,
            PlanNode::ListDatabases { cost, .. } => *cost,
            PlanNode::TableScan { cost, .. } => *cost,
            PlanNode::CreateTable { cost, .. } => *cost,
            PlanNode::DropTable { cost, .. } => *cost,
            PlanNode::ListTables { cost, .. } => *cost,
            PlanNode::Get { cost, .. } => *cost,
            PlanNode::GetAll { cost, .. } => *cost,
            PlanNode::Insert { cost, .. } => *cost,
            PlanNode::Update { cost, .. } => *cost,
            PlanNode::Delete { cost, .. } => *cost,
            PlanNode::Filter { cost, .. } => *cost,
            PlanNode::OrderBy { cost, .. } => *cost,
            PlanNode::Limit { cost, .. } => *cost,
            PlanNode::Skip { cost, .. } => *cost,
            PlanNode::Count { cost, .. } => *cost,
            PlanNode::Subquery { cost, .. } => *cost,
        }
    }

    /// Get the estimated number of rows this node will produce
    pub fn estimated_rows(&self) -> f64 {
        match self {
            PlanNode::Constant { .. } => 1.0,
            PlanNode::CreateDatabase { .. } => 0.0,
            PlanNode::DropDatabase { .. } => 0.0,
            PlanNode::ListDatabases { .. } => 10.0, // Assume 10 databases on average
            PlanNode::TableScan { estimated_rows, .. } => *estimated_rows,
            PlanNode::CreateTable { .. } => 0.0,
            PlanNode::DropTable { .. } => 0.0,
            PlanNode::ListTables { .. } => 50.0, // Assume 50 tables on average
            PlanNode::Get { .. } => 1.0,
            PlanNode::GetAll { keys, .. } => keys.len() as f64,
            PlanNode::Insert { documents, .. } => documents.len() as f64,
            PlanNode::Update { source, .. } => source.estimated_rows(),
            PlanNode::Delete { source, .. } => source.estimated_rows(),
            PlanNode::Filter {
                source,
                selectivity,
                ..
            } => source.estimated_rows() * selectivity,
            PlanNode::OrderBy { source, .. } => source.estimated_rows(),
            PlanNode::Limit { source, count, .. } => source.estimated_rows().min(*count as f64),
            PlanNode::Skip { source, count, .. } => {
                (source.estimated_rows() - *count as f64).max(0.0)
            }
            PlanNode::Count { .. } => 1.0,
            PlanNode::Subquery { query, .. } => query.estimated_rows(),
        }
    }
}

impl PartialEq for PlanNode {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (PlanNode::Constant { value: v1, .. }, PlanNode::Constant { value: v2, .. }) => {
                v1 == v2
            }
            (
                PlanNode::CreateDatabase { name: n1, .. },
                PlanNode::CreateDatabase { name: n2, .. },
            ) => n1 == n2,
            (PlanNode::DropDatabase { name: n1, .. }, PlanNode::DropDatabase { name: n2, .. }) => {
                n1 == n2
            }
            (
                PlanNode::ListDatabases { cursor: c1, .. },
                PlanNode::ListDatabases { cursor: c2, .. },
            ) => c1 == c2,
            (
                PlanNode::TableScan {
                    table_ref: t1,
                    filter: f1,
                    ..
                },
                PlanNode::TableScan {
                    table_ref: t2,
                    filter: f2,
                    ..
                },
            ) => t1 == t2 && f1 == f2,
            (
                PlanNode::CreateTable { table_ref: t1, .. },
                PlanNode::CreateTable { table_ref: t2, .. },
            ) => t1 == t2,
            (
                PlanNode::DropTable { table_ref: t1, .. },
                PlanNode::DropTable { table_ref: t2, .. },
            ) => t1 == t2,
            (
                PlanNode::ListTables {
                    database_ref: d1, ..
                },
                PlanNode::ListTables {
                    database_ref: d2, ..
                },
            ) => d1 == d2,
            (
                PlanNode::Get {
                    table_ref: t1,
                    key: k1,
                    ..
                },
                PlanNode::Get {
                    table_ref: t2,
                    key: k2,
                    ..
                },
            ) => t1 == t2 && k1 == k2,
            (
                PlanNode::GetAll {
                    table_ref: t1,
                    keys: k1,
                    ..
                },
                PlanNode::GetAll {
                    table_ref: t2,
                    keys: k2,
                    ..
                },
            ) => t1 == t2 && k1 == k2,
            (
                PlanNode::Insert {
                    table_ref: t1,
                    documents: d1,
                    ..
                },
                PlanNode::Insert {
                    table_ref: t2,
                    documents: d2,
                    ..
                },
            ) => t1 == t2 && d1 == d2,
            (
                PlanNode::Update {
                    source: s1,
                    patch: p1,
                    ..
                },
                PlanNode::Update {
                    source: s2,
                    patch: p2,
                    ..
                },
            ) => s1 == s2 && p1 == p2,
            (PlanNode::Delete { source: s1, .. }, PlanNode::Delete { source: s2, .. }) => s1 == s2,
            (
                PlanNode::Filter {
                    source: s1,
                    predicate: p1,
                    ..
                },
                PlanNode::Filter {
                    source: s2,
                    predicate: p2,
                    ..
                },
            ) => s1 == s2 && p1 == p2,
            (
                PlanNode::OrderBy {
                    source: s1,
                    fields: f1,
                    ..
                },
                PlanNode::OrderBy {
                    source: s2,
                    fields: f2,
                    ..
                },
            ) => s1 == s2 && f1 == f2,
            (
                PlanNode::Limit {
                    source: s1,
                    count: c1,
                    ..
                },
                PlanNode::Limit {
                    source: s2,
                    count: c2,
                    ..
                },
            ) => s1 == s2 && c1 == c2,
            (
                PlanNode::Skip {
                    source: s1,
                    count: c1,
                    ..
                },
                PlanNode::Skip {
                    source: s2,
                    count: c2,
                    ..
                },
            ) => s1 == s2 && c1 == c2,
            (PlanNode::Count { source: s1, .. }, PlanNode::Count { source: s2, .. }) => s1 == s2,
            (PlanNode::Subquery { query: q1, .. }, PlanNode::Subquery { query: q2, .. }) => {
                q1 == q2
            }
            _ => false,
        }
    }
}
