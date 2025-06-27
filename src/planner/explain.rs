use crate::ast::*;
use crate::planner::node::PlanNode;
use std::fmt;

/// Represents a complete explanation of a query plan
#[derive(Debug)]
pub struct PlanExplanation {
    pub nodes: Vec<ExplanationNode>,
    pub total_cost: f64,
    pub estimated_rows: f64,
}

/// Represents a single node in the plan explanation
#[derive(Debug)]
pub struct ExplanationNode {
    pub operation: String,
    pub properties: Vec<(String, String)>,
    pub cost: f64,
    pub estimated_rows: f64,
    pub depth: usize,
}

impl PlanExplanation {
    /// Create a new plan explanation
    pub fn new(root: &PlanNode) -> Self {
        let mut nodes = Vec::new();
        let mut explainer = Explainer::new();
        explainer.explain_node(root, 0, &mut nodes);

        Self {
            nodes,
            total_cost: root.cost(),
            estimated_rows: root.estimated_rows(),
        }
    }
}

impl fmt::Display for PlanExplanation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Query Plan:")?;
        writeln!(
            f,
            "Total Cost: {:.2}, Estimated Rows: {:.0}",
            self.total_cost, self.estimated_rows
        )?;
        writeln!(f, "{}", "-".repeat(80))?;

        for node in &self.nodes {
            // Indent based on depth
            let indent = "  ".repeat(node.depth);
            writeln!(
                f,
                "{}{} (cost={:.2}, rows={:.0})",
                indent, node.operation, node.cost, node.estimated_rows
            )?;

            // Print properties
            for (key, value) in &node.properties {
                writeln!(f, "{indent}  {key}: {value}")?;
            }
        }

        Ok(())
    }
}

/// Helper struct for building plan explanations
struct Explainer;

impl Explainer {
    fn new() -> Self {
        Self
    }

    fn explain_node(&mut self, node: &PlanNode, depth: usize, nodes: &mut Vec<ExplanationNode>) {
        let (operation, properties) = self.get_node_info(node);

        nodes.push(ExplanationNode {
            operation,
            properties,
            cost: node.cost(),
            estimated_rows: node.estimated_rows(),
            depth,
        });

        // Recursively explain child nodes
        match node {
            PlanNode::Update { source, .. }
            | PlanNode::Delete { source, .. }
            | PlanNode::Filter { source, .. }
            | PlanNode::OrderBy { source, .. }
            | PlanNode::Limit { source, .. }
            | PlanNode::Skip { source, .. }
            | PlanNode::Count { source, .. } => {
                self.explain_node(source, depth + 1, nodes);
            }
            PlanNode::Subquery { query, .. } => {
                self.explain_node(query, depth + 1, nodes);
            }
            _ => {}
        }
    }

    fn get_node_info(&self, node: &PlanNode) -> (String, Vec<(String, String)>) {
        match node {
            PlanNode::Constant { value, .. } => {
                let value_str = format!("{value:?}");
                (
                    "Constant".to_string(),
                    vec![("Value".to_string(), value_str)],
                )
            }
            PlanNode::CreateDatabase { name, .. } => (
                "CreateDatabase".to_string(),
                vec![("Database".to_string(), name.clone())],
            ),
            PlanNode::DropDatabase { name, .. } => (
                "DropDatabase".to_string(),
                vec![("Database".to_string(), name.clone())],
            ),
            PlanNode::ListDatabases { cursor, .. } => {
                let mut props = vec![];
                if let Some(cursor) = cursor {
                    if let Some(batch_size) = cursor.batch_size {
                        props.push(("BatchSize".to_string(), batch_size.to_string()));
                    }
                    if let Some(start_key) = &cursor.start_key {
                        props.push(("StartKey".to_string(), start_key.clone()));
                    }
                }
                ("ListDatabases".to_string(), props)
            }
            PlanNode::TableScan {
                table_ref,
                cursor,
                filter,
                ..
            } => {
                let mut props = vec![(
                    "Table".to_string(),
                    format!(
                        "{}.{}",
                        table_ref
                            .database
                            .as_ref()
                            .map(|d| d.name.as_str())
                            .unwrap_or("default"),
                        table_ref.name
                    ),
                )];

                if let Some(cursor) = cursor {
                    if let Some(batch_size) = cursor.batch_size {
                        props.push(("BatchSize".to_string(), batch_size.to_string()));
                    }
                    if let Some(start_key) = &cursor.start_key {
                        props.push(("StartKey".to_string(), start_key.clone()));
                    }
                }

                if let Some(filter) = filter {
                    props.push(("Filter".to_string(), self.describe_predicate(filter)));
                }

                ("TableScan".to_string(), props)
            }
            PlanNode::CreateTable { table_ref, .. } => (
                "CreateTable".to_string(),
                vec![(
                    "Table".to_string(),
                    format!(
                        "{}.{}",
                        table_ref
                            .database
                            .as_ref()
                            .map(|d| d.name.as_str())
                            .unwrap_or("default"),
                        table_ref.name
                    ),
                )],
            ),
            PlanNode::DropTable { table_ref, .. } => (
                "DropTable".to_string(),
                vec![(
                    "Table".to_string(),
                    format!(
                        "{}.{}",
                        table_ref
                            .database
                            .as_ref()
                            .map(|d| d.name.as_str())
                            .unwrap_or("default"),
                        table_ref.name
                    ),
                )],
            ),
            PlanNode::ListTables {
                database_ref,
                cursor,
                ..
            } => {
                let mut props = vec![("Database".to_string(), database_ref.name.clone())];

                if let Some(cursor) = cursor {
                    if let Some(batch_size) = cursor.batch_size {
                        props.push(("BatchSize".to_string(), batch_size.to_string()));
                    }
                    if let Some(start_key) = &cursor.start_key {
                        props.push(("StartKey".to_string(), start_key.clone()));
                    }
                }

                ("ListTables".to_string(), props)
            }
            PlanNode::Get { table_ref, key, .. } => (
                "Get".to_string(),
                vec![
                    (
                        "Table".to_string(),
                        format!(
                            "{}.{}",
                            table_ref
                                .database
                                .as_ref()
                                .map(|d| d.name.as_str())
                                .unwrap_or("default"),
                            table_ref.name
                        ),
                    ),
                    ("Key".to_string(), key.clone()),
                ],
            ),
            PlanNode::GetAll {
                table_ref,
                keys,
                cursor,
                ..
            } => {
                let mut props = vec![
                    (
                        "Table".to_string(),
                        format!(
                            "{}.{}",
                            table_ref
                                .database
                                .as_ref()
                                .map(|d| d.name.as_str())
                                .unwrap_or("default"),
                            table_ref.name
                        ),
                    ),
                    ("Keys".to_string(), format!("{} keys", keys.len())),
                ];

                if let Some(cursor) = cursor {
                    if let Some(batch_size) = cursor.batch_size {
                        props.push(("BatchSize".to_string(), batch_size.to_string()));
                    }
                    if let Some(start_key) = &cursor.start_key {
                        props.push(("StartKey".to_string(), start_key.clone()));
                    }
                }

                ("GetAll".to_string(), props)
            }
            PlanNode::Insert {
                table_ref,
                documents,
                ..
            } => (
                "Insert".to_string(),
                vec![
                    (
                        "Table".to_string(),
                        format!(
                            "{}.{}",
                            table_ref
                                .database
                                .as_ref()
                                .map(|d| d.name.as_str())
                                .unwrap_or("default"),
                            table_ref.name
                        ),
                    ),
                    (
                        "Documents".to_string(),
                        format!("{} documents", documents.len()),
                    ),
                ],
            ),
            PlanNode::Update { patch, .. } => {
                let patch_str = format!("{patch:?}");
                ("Update".to_string(), vec![("Patch".to_string(), patch_str)])
            }
            PlanNode::Delete { .. } => ("Delete".to_string(), vec![]),
            PlanNode::Filter {
                predicate,
                selectivity,
                ..
            } => (
                "Filter".to_string(),
                vec![
                    ("Predicate".to_string(), self.describe_predicate(predicate)),
                    ("Selectivity".to_string(), format!("{selectivity:.2}")),
                ],
            ),
            PlanNode::OrderBy { fields, .. } => {
                let fields_str = fields
                    .iter()
                    .map(|f| {
                        format!(
                            "{} {}",
                            f.field_name,
                            if f.ascending { "ASC" } else { "DESC" }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                (
                    "OrderBy".to_string(),
                    vec![("Fields".to_string(), fields_str)],
                )
            }
            PlanNode::Limit { count, .. } => (
                "Limit".to_string(),
                vec![("Count".to_string(), count.to_string())],
            ),
            PlanNode::Skip { count, .. } => (
                "Skip".to_string(),
                vec![("Count".to_string(), count.to_string())],
            ),
            PlanNode::Count { .. } => ("Count".to_string(), vec![]),
            PlanNode::Subquery { .. } => ("Subquery".to_string(), vec![]),
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    fn describe_predicate(&self, expr: &Expression) -> String {
        match &expr.expr {
            Some(expression::Expr::Literal(lit)) => format!("{:?}", lit.value),
            Some(expression::Expr::Field(field)) => field.path.join("."),
            Some(expression::Expr::Binary(bin)) => {
                let left_str = bin
                    .left
                    .as_ref()
                    .map(|l| self.describe_predicate(l))
                    .unwrap_or_else(|| "NULL".to_string());
                let right_str = bin
                    .right
                    .as_ref()
                    .map(|r| self.describe_predicate(r))
                    .unwrap_or_else(|| "NULL".to_string());
                let op_str = binary_op::Operator::try_from(bin.op)
                    .map(|op| format!("{op:?}"))
                    .unwrap_or_else(|_| "UNKNOWN".to_string());
                format!("{left_str} {op_str} {right_str}")
            }
            Some(expression::Expr::Unary(un)) => {
                let operand_str = un
                    .expr
                    .as_ref()
                    .map(|e| self.describe_predicate(e))
                    .unwrap_or_else(|| "NULL".to_string());
                let op_str = unary_op::Operator::try_from(un.op)
                    .map(|op| format!("{op:?}"))
                    .unwrap_or_else(|_| "UNKNOWN".to_string());
                format!("{op_str} {operand_str}")
            }
            Some(expression::Expr::Subquery(_)) => "SUBQUERY".to_string(),
            Some(expression::Expr::Variable(var)) => format!("${}", var.name),
            Some(expression::Expr::Match(_)) => "MATCH".to_string(),
            None => "EMPTY".to_string(),
        }
    }
}
