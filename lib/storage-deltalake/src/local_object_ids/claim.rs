use crate::local_object_ids::error::LocalObjectIdError;
use std::sync::Arc;

#[async_trait::async_trait]
pub trait ObjectIdClaimer: std::fmt::Debug + Send + Sync {
    async fn claim_next_range(&self) -> Result<(i64, i64), LocalObjectIdError>;
}

#[derive(Debug, Clone)]
pub struct StaticObjectIdClaimer;

#[async_trait::async_trait]
impl ObjectIdClaimer for StaticObjectIdClaimer {
    async fn claim_next_range(&self) -> Result<(i64, i64), LocalObjectIdError> {
        Ok((0, i64::MAX))
    }
}

/// Represents the claimed object ids of a node.
#[derive(Debug, Clone)]
pub struct ObjectIdClaim {
    /// The state holds the range `[next_free, last_free]` where both are inclusive.
    claim_state: Option<(i64, i64)>,
    claimer: Option<Arc<dyn ObjectIdClaimer>>,
}

pub struct NextIdResult {
    pub id: i64,
    pub newly_claimed: Option<(i64, i64)>,
}

impl ObjectIdClaim {
    /// Creates a new [`ObjectIdClaim`].
    pub fn new(
        initial_claim: Option<(i64, i64)>,
        claimer: Option<Arc<dyn ObjectIdClaimer>>,
    ) -> Self {
        Self {
            claim_state: initial_claim,
            claimer,
        }
    }

    /// Returns the current object id claim.
    pub fn peek_current_claim(&self) -> Option<(i64, i64)> {
        self.claim_state
    }

    pub async fn acquire_next_id(&mut self) -> Result<NextIdResult, LocalObjectIdError> {
        if let Some((next_free, last_free)) = self.claim_state {
            if next_free < last_free {
                self.claim_state = Some((next_free + 1, last_free));
                return Ok(NextIdResult {
                    id: next_free,
                    newly_claimed: None,
                });
            } else if next_free == last_free {
                self.claim_state = None
            } else {
                return Err(LocalObjectIdError::ObjectIdClaimer(
                    "Invalid range in object id claim.".to_string(),
                ));
            }
        }

        let Some(ref claimer) = self.claimer else {
            return Err(LocalObjectIdError::ObjectIdClaimer(
                "ObjectIdClaimer is required to claim new ID range but none was configured".to_string(),
            ));
        };

        let (next_free, last_free) = claimer.claim_next_range().await?;

        if next_free < last_free {
            self.claim_state = Some((next_free + 1, last_free));
        } else if next_free == last_free {
            self.claim_state = None;
        } else {
            return Err(LocalObjectIdError::ObjectIdClaimer(
                "Invalid claim retruned from object id claimer.".to_string(),
            ));
        }

        Ok(NextIdResult {
            newly_claimed: Some((next_free, last_free)),
            id: next_free,
        })
    }

    pub fn set_claim_state(&mut self, state: Option<(i64, i64)>) {
        self.claim_state = state
    }
}
