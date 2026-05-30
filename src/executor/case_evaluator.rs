//! Case/when/then evaluation.
//!
//! Worker-specific control flow.  The condition primitives
//! (`Operator`, `Condition`, `evaluate_structured_condition`) live in
//! `noetl-executor`'s shared `condition` module so the CLI's local-mode
//! runner and the worker's NATS pull consumer agree on operator
//! semantics.  See § H.10 of Appendix H of the global hybrid cloud
//! blueprint for the architectural rationale.
//!
//! What stays here (worker-specific):
//! - `Case` — one when/then entry from the playbook.
//! - `CaseAction` — what to do when a case matches: Continue, Exit,
//!   SetVar, Goto, Retry, Fail.  These are pull-loop dispatch
//!   semantics; the CLI's tree walker has its own equivalent.
//! - `CaseResult` — the dispatcher's outcome.
//! - `CaseEvaluator` — the dispatcher that iterates cases and finds
//!   the first match.
//!
//! What moved to `noetl-executor` (R-1.2 PR-2c):
//! - `Operator` enum — re-exported below for backward compatibility.
//! - `Condition` struct — re-exported below.
//! - `evaluate_structured_condition` — called from
//!   `CaseEvaluator::evaluate_conditions`.
//! - `resolve_value`, `resolve_json_value`, `json_path`,
//!   `compare_numeric`, `is_truthy`, `value_to_f64` — all moved as
//!   private helpers in the executor's condition module.

use anyhow::Result;
use noetl_executor::condition::evaluate_structured_condition;
use noetl_tools::context::ExecutionContext;
use serde::{Deserialize, Serialize};

/// R-1.2 PR-2c: re-export the condition operator + envelope from
/// `noetl_executor::condition` so callers using
/// `crate::executor::case_evaluator::{Operator, Condition}` paths
/// continue to compile.  The structured-condition primitives now
/// live in the shared executor crate; the worker keeps the case
/// dispatcher (`Case` / `CaseAction` / `CaseEvaluator` / `CaseResult`)
/// because that's the pull-loop control flow specific to worker
/// dispatch semantics per § H.10.
#[allow(unused_imports)] // re-exports are for external callers + tests
pub use noetl_executor::condition::{Condition, Operator};

/// Case specification with when/then.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Case {
    /// Condition(s) to evaluate.
    #[serde(rename = "when")]
    pub conditions: Vec<Condition>,

    /// Action to take if conditions match.
    pub then: CaseAction,
}

/// Action to take when a case matches.
///
/// Worker-specific control flow.  Each variant maps to a dispatch
/// decision the worker makes after evaluating the case's conditions.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaseAction {
    /// Continue to next step.
    Continue,
    /// Exit step with status.
    Exit {
        status: String,
        data: Option<serde_json::Value>,
    },
    /// Set a variable.
    SetVar {
        name: String,
        value: serde_json::Value,
    },
    /// Jump to another step.
    Goto { step: String },
    /// Retry the current call.
    Retry { delay_ms: Option<u64> },
    /// Fail the command.
    Fail { message: String },
}

/// Result of case evaluation.
#[derive(Debug, Clone)]
pub struct CaseResult {
    /// The matched case index.
    pub case_index: usize,

    /// The action to take.
    pub action: CaseAction,
}

/// Evaluates case/when/then conditions.
///
/// R-1.2 PR-2c: the per-condition evaluation delegates to
/// `noetl_executor::condition::evaluate_structured_condition`.  The
/// outer case iteration + first-match-wins semantics stay here
/// because they describe what the worker does AFTER a match (and the
/// CLI's tree walker has its own equivalent that doesn't fit this
/// shape).
#[derive(Default)]
pub struct CaseEvaluator;

impl CaseEvaluator {
    /// Create a new case evaluator.
    pub fn new() -> Self {
        Self
    }

    /// Evaluate cases against the execution context and tool result.
    ///
    /// Returns the first matching case or None if no case matches.
    pub fn evaluate(
        &self,
        cases: &[Case],
        ctx: &ExecutionContext,
        result: Option<&serde_json::Value>,
    ) -> Result<Option<CaseResult>> {
        for (index, case) in cases.iter().enumerate() {
            if self.evaluate_conditions(&case.conditions, ctx, result)? {
                return Ok(Some(CaseResult {
                    case_index: index,
                    action: case.then.clone(),
                }));
            }
        }

        Ok(None)
    }

    /// Evaluate a set of conditions (AND logic).
    ///
    /// Returns `true` only if every condition matches.
    fn evaluate_conditions(
        &self,
        conditions: &[Condition],
        ctx: &ExecutionContext,
        result: Option<&serde_json::Value>,
    ) -> Result<bool> {
        for condition in conditions {
            if !evaluate_structured_condition(condition, ctx, result)? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_case_evaluator_eq() {
        let evaluator = CaseEvaluator::new();
        let mut ctx = ExecutionContext::default();
        ctx.set_variable("status", serde_json::json!("success"));

        let cases = vec![Case {
            conditions: vec![Condition {
                left: "status".to_string(),
                op: Operator::Eq,
                right: Some(serde_json::json!("success")),
            }],
            then: CaseAction::Continue,
        }];

        let result = evaluator.evaluate(&cases, &ctx, None).unwrap();
        assert!(result.is_some());
        assert!(matches!(result.unwrap().action, CaseAction::Continue));
    }

    #[test]
    fn test_case_evaluator_gt() {
        let evaluator = CaseEvaluator::new();
        let mut ctx = ExecutionContext::default();
        ctx.set_variable("count", serde_json::json!(10));

        let cases = vec![Case {
            conditions: vec![Condition {
                left: "count".to_string(),
                op: Operator::Gt,
                right: Some(serde_json::json!(5)),
            }],
            then: CaseAction::Continue,
        }];

        let result = evaluator.evaluate(&cases, &ctx, None).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_case_evaluator_contains() {
        let evaluator = CaseEvaluator::new();
        let mut ctx = ExecutionContext::default();
        ctx.set_variable("message", serde_json::json!("hello world"));

        let cases = vec![Case {
            conditions: vec![Condition {
                left: "message".to_string(),
                op: Operator::Contains,
                right: Some(serde_json::json!("world")),
            }],
            then: CaseAction::Continue,
        }];

        let result = evaluator.evaluate(&cases, &ctx, None).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_case_evaluator_result_path() {
        let evaluator = CaseEvaluator::new();
        let ctx = ExecutionContext::default();
        let result = serde_json::json!({
            "status": "ok",
            "data": {"count": 42}
        });

        let cases = vec![Case {
            conditions: vec![Condition {
                left: "result.status".to_string(),
                op: Operator::Eq,
                right: Some(serde_json::json!("ok")),
            }],
            then: CaseAction::Continue,
        }];

        let eval_result = evaluator.evaluate(&cases, &ctx, Some(&result)).unwrap();
        assert!(eval_result.is_some());
    }

    #[test]
    fn test_case_evaluator_first_match_wins() {
        // Worker-specific control flow: when multiple cases would
        // match, only the first is returned.  This locks in the
        // ordering contract the dispatcher relies on.
        let evaluator = CaseEvaluator::new();
        let mut ctx = ExecutionContext::default();
        ctx.set_variable("status", serde_json::json!("ok"));

        let cases = vec![
            Case {
                conditions: vec![Condition {
                    left: "status".to_string(),
                    op: Operator::Eq,
                    right: Some(serde_json::json!("ok")),
                }],
                then: CaseAction::Continue,
            },
            Case {
                conditions: vec![Condition {
                    left: "status".to_string(),
                    op: Operator::Eq,
                    right: Some(serde_json::json!("ok")),
                }],
                then: CaseAction::Fail {
                    message: "should not reach".to_string(),
                },
            },
        ];

        let result = evaluator.evaluate(&cases, &ctx, None).unwrap().unwrap();
        assert_eq!(result.case_index, 0);
        assert!(matches!(result.action, CaseAction::Continue));
    }

    #[test]
    fn test_case_evaluator_no_match_returns_none() {
        let evaluator = CaseEvaluator::new();
        let mut ctx = ExecutionContext::default();
        ctx.set_variable("status", serde_json::json!("nope"));

        let cases = vec![Case {
            conditions: vec![Condition {
                left: "status".to_string(),
                op: Operator::Eq,
                right: Some(serde_json::json!("ok")),
            }],
            then: CaseAction::Continue,
        }];

        assert!(evaluator.evaluate(&cases, &ctx, None).unwrap().is_none());
    }

    #[test]
    fn test_case_evaluator_and_semantics() {
        // Multiple conditions on one case = AND.
        let evaluator = CaseEvaluator::new();
        let mut ctx = ExecutionContext::default();
        ctx.set_variable("a", serde_json::json!(1));
        ctx.set_variable("b", serde_json::json!(2));

        let cases_both_match = vec![Case {
            conditions: vec![
                Condition {
                    left: "a".to_string(),
                    op: Operator::Eq,
                    right: Some(serde_json::json!(1)),
                },
                Condition {
                    left: "b".to_string(),
                    op: Operator::Eq,
                    right: Some(serde_json::json!(2)),
                },
            ],
            then: CaseAction::Continue,
        }];
        assert!(evaluator
            .evaluate(&cases_both_match, &ctx, None)
            .unwrap()
            .is_some());

        let cases_one_fails = vec![Case {
            conditions: vec![
                Condition {
                    left: "a".to_string(),
                    op: Operator::Eq,
                    right: Some(serde_json::json!(1)),
                },
                Condition {
                    left: "b".to_string(),
                    op: Operator::Eq,
                    right: Some(serde_json::json!(99)),
                },
            ],
            then: CaseAction::Continue,
        }];
        assert!(evaluator
            .evaluate(&cases_one_fails, &ctx, None)
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_case_action_serialization() {
        let action = CaseAction::Exit {
            status: "completed".to_string(),
            data: Some(serde_json::json!({"result": 42})),
        };

        let json = serde_json::to_string(&action).unwrap();
        assert!(json.contains("exit"));
        assert!(json.contains("completed"));
    }
}
