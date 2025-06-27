use pcre2::bytes::Regex;

use crate::ast::{
    BinaryOp, Datum, Expression, FieldRef, MatchExpr, UnaryOp, Variable,
    binary_op::Operator as BinaryOperator, datum, expression, unary_op::Operator as UnaryOperator,
};
use crate::evaluator::error::EvalError;
use crate::evaluator::utils::{
    bool_datum, compare_values, datum_to_bool, datums_equal, extract_field_from_ref,
};

/// Handler for evaluating expressions based on the proto-defined Expression structure
pub struct ExpressionEvaluator;

impl ExpressionEvaluator {
    /// Create a new expression evaluator
    pub fn new() -> Self {
        Self
    }

    /// Evaluate an expression against a datum context
    pub fn evaluate_expression(
        &self,
        expr: &Expression,
        context: &Datum,
    ) -> Result<Datum, EvalError> {
        match &expr.expr {
            Some(expression::Expr::Literal(datum)) => Ok(datum.clone()),
            Some(expression::Expr::Field(f)) => Ok(self.evaluate_field_reference(f, context)),
            Some(expression::Expr::Variable(var)) => self.evaluate_variable(var, context),
            Some(expression::Expr::Binary(op)) => self.evaluate_binary_operation(op, context),
            Some(expression::Expr::Unary(op)) => self.evaluate_unary_operation(op, context),
            Some(expression::Expr::Match(ex)) => self.evaluate_match_expression(ex, context),
            Some(expression::Expr::Subquery(q)) => self.evaluate_simple_subquery(q, context),
            None => Err(EvalError::InvalidExpression),
        }
    }

    /// Evaluate a field reference expression
    fn evaluate_field_reference(&self, field_ref: &FieldRef, context: &Datum) -> Datum {
        extract_field_from_ref(context, field_ref)
    }

    /// Evaluate a variable expression
    fn evaluate_variable(&self, variable: &Variable, context: &Datum) -> Result<Datum, EvalError> {
        // Variables would typically be resolved from some context/scope
        // For now, we'll treat them as field references with the variable name
        let field_ref = FieldRef {
            path: vec![variable.name.clone()],
            separator: String::new(),
        };
        Ok(self.evaluate_field_reference(&field_ref, context))
    }

    /// Evaluate a binary operation
    fn evaluate_binary_operation(
        &self,
        binary_op: &BinaryOp,
        context: &Datum,
    ) -> Result<Datum, EvalError> {
        let left = self.evaluate_expression(binary_op.left.as_ref().unwrap(), context)?;
        let right = self.evaluate_expression(binary_op.right.as_ref().unwrap(), context)?;

        let operator =
            BinaryOperator::try_from(binary_op.op).map_err(|_| EvalError::InvalidExpression)?;

        self.perform_binary_operation(&left, &operator, &right)
    }

    /// Evaluate a unary operation
    fn evaluate_unary_operation(
        &self,
        unary_op: &UnaryOp,
        context: &Datum,
    ) -> Result<Datum, EvalError> {
        let operand = self.evaluate_expression(unary_op.expr.as_ref().unwrap(), context)?;

        let operator =
            UnaryOperator::try_from(unary_op.op).map_err(|_| EvalError::InvalidExpression)?;

        self.perform_unary_operation(&operator, &operand)
    }

    /// Evaluate a match expression
    fn evaluate_match_expression(
        &self,
        match_expr: &MatchExpr,
        context: &Datum,
    ) -> Result<Datum, EvalError> {
        let value = self.evaluate_expression(match_expr.value.as_ref().unwrap(), context)?;

        // Simple pattern matching - could be extended for regex patterns
        match (&value.value, &match_expr.pattern, &match_expr.flags) {
            (Some(datum::Value::String(text)), pattern, flags) => {
                let is_match = self.matches_pattern(text, pattern, flags);
                Ok(bool_datum(is_match))
            }
            _ => Err(EvalError::InvalidMatchValue(value)),
        }
    }

    /// Perform a binary operation between two datums
    pub fn perform_binary_operation(
        &self,
        left: &Datum,
        operator: &BinaryOperator,
        right: &Datum,
    ) -> Result<Datum, EvalError> {
        match operator {
            // Comparison operators
            BinaryOperator::Eq => Ok(bool_datum(datums_equal(left, right))),
            BinaryOperator::Ne => Ok(bool_datum(!datums_equal(left, right))),
            BinaryOperator::Lt => self.perform_comparison(left, right, |ord| ord.is_lt()),
            BinaryOperator::Le => self.perform_comparison(left, right, |ord| ord.is_le()),
            BinaryOperator::Gt => self.perform_comparison(left, right, |ord| ord.is_gt()),
            BinaryOperator::Ge => self.perform_comparison(left, right, |ord| ord.is_ge()),

            // Logical operators
            BinaryOperator::And => self.perform_logical_and(left, right),
            BinaryOperator::Or => self.perform_logical_or(left, right),
        }
    }

    /// Perform a unary operation on a datum
    pub fn perform_unary_operation(
        &self,
        operator: &UnaryOperator,
        operand: &Datum,
    ) -> Result<Datum, EvalError> {
        match operator {
            UnaryOperator::Not => Ok(bool_datum(!datum_to_bool(operand))),
        }
    }

    /// Perform comparison operations
    fn perform_comparison<F>(
        &self,
        left: &Datum,
        right: &Datum,
        compare_fn: F,
    ) -> Result<Datum, EvalError>
    where
        F: Fn(std::cmp::Ordering) -> bool,
    {
        let ordering = compare_values(left, right);
        Ok(bool_datum(compare_fn(ordering)))
    }

    /// Perform logical AND operation
    fn perform_logical_and(&self, left: &Datum, right: &Datum) -> Result<Datum, EvalError> {
        let left_bool = datum_to_bool(left);
        if !left_bool {
            return Ok(bool_datum(false));
        }
        let right_bool = datum_to_bool(right);
        Ok(bool_datum(left_bool && right_bool))
    }

    /// Perform logical OR operation
    fn perform_logical_or(&self, left: &Datum, right: &Datum) -> Result<Datum, EvalError> {
        let left_bool = datum_to_bool(left);
        if left_bool {
            return Ok(bool_datum(true));
        }
        let right_bool = datum_to_bool(right);
        Ok(bool_datum(left_bool || right_bool))
    }

    /// Evaluate a simple subquery expression
    fn evaluate_simple_subquery(
        &self,
        query: &crate::ast::Query,
        context: &Datum,
    ) -> Result<Datum, EvalError> {
        match &query.kind {
            Some(crate::ast::query::Kind::Expression(expr)) => {
                self.evaluate_expression(expr, context)
            }
            _ => Err(EvalError::UnsupportedOperation),
        }
    }

    /// Simple pattern matching for match expressions
    fn matches_pattern(&self, text: &str, pattern: &str, flags: &str) -> bool {
        let mut pattern_str = String::from(flags);
        pattern_str.push_str(pattern);

        // Compile the regex
        match Regex::new(&pattern_str) {
            Ok(re) => re.is_match(text.as_bytes()).unwrap_or(false),
            Err(err) => {
                log::debug!("Regex compile error: {err}");
                false
            }
        }
    }

    /// Check if an expression evaluates to a boolean value
    pub fn is_boolean_expression(&self, expr: &Expression) -> bool {
        match &expr.expr {
            Some(expression::Expr::Literal(datum)) => {
                matches!(datum.value, Some(datum::Value::Bool(_)))
            }
            Some(expression::Expr::Binary(binary_op)) => {
                let operator = BinaryOperator::try_from(binary_op.op).unwrap_or(BinaryOperator::Eq);
                matches!(
                    operator,
                    BinaryOperator::Eq
                        | BinaryOperator::Ne
                        | BinaryOperator::Lt
                        | BinaryOperator::Le
                        | BinaryOperator::Gt
                        | BinaryOperator::Ge
                        | BinaryOperator::And
                        | BinaryOperator::Or
                )
            }
            Some(expression::Expr::Unary(unary_op)) => {
                matches!(UnaryOperator::try_from(unary_op.op), Ok(UnaryOperator::Not))
            }
            Some(expression::Expr::Match(_)) => true,
            _ => false,
        }
    }
}

impl Default for ExpressionEvaluator {
    fn default() -> Self {
        Self::new()
    }
}
