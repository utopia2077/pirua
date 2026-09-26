//! Inert selected-core guard and borrowed active Envelope view.
//!
//! Its input is a successful full v1.1 Envelope scan. Endpoint and the live
//! pump still use Prost; this module does not admit tool values or debit credit.

use std::num::NonZeroUsize;

use super::echo_bounded::{
    BodySpan, BorrowedIndexArena, IndexedMessage, MessageId, ScanError, SelectedMinor, SizeError,
};
use super::value::{self, Projection};
use crate::jcs;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActiveArm {
    Request,
    Response,
    StreamItem,
    StreamEnd,
    Cancel,
    Credit,
    GoAway,
}

impl ActiveArm {
    fn from_tag(tag: u32) -> Option<Self> {
        Some(match tag {
            6 => Self::Request,
            7 => Self::Response,
            8 => Self::StreamItem,
            9 => Self::StreamEnd,
            10 => Self::Cancel,
            11 => Self::Credit,
            12 => Self::GoAway,
            _ => return None,
        })
    }

    fn tag(self) -> u32 {
        match self {
            Self::Request => 6,
            Self::Response => 7,
            Self::StreamItem => 8,
            Self::StreamEnd => 9,
            Self::Cancel => 10,
            Self::Credit => 11,
            Self::GoAway => 12,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SelectError {
    UnexpectedFrame,
    FrameTooLarge {
        length: u64,
        limit: usize,
    },
    /// Generated-field policy drift or checked-size arithmetic failure.
    InternalPolicy,
}

/// The borrowed effective active arm and the transmitted body length. The
/// view owns the arena borrow; an opaque span can outlive it and reborrow the
/// original body when the arena is reused for the inner payload scan.
pub struct SelectedActive<'view, 'body> {
    view: IndexedMessage<'view, 'body>,
    arm: ActiveArm,
    normalized_len: Option<usize>,
}

impl<'view, 'body> SelectedActive<'view, 'body> {
    pub fn arm(&self) -> ActiveArm {
        self.arm
    }

    pub fn body_len(&self) -> usize {
        self.view.body_len()
    }

    /// The v1.1 DTO reencoded length checked for selected core 1.0.
    pub fn normalized_len(&self) -> Option<usize> {
        self.normalized_len
    }

    pub fn view(&self) -> &IndexedMessage<'view, 'body> {
        &self.view
    }

    /// Ordinary proto3 scalar defaults apply only to genuinely absent fields.
    pub fn request_id(&self) -> Result<u64, RoleError> {
        self.header_scalar(1)
    }

    pub fn sequence(&self) -> Result<u64, RoleError> {
        if self.arm != ActiveArm::StreamItem {
            return Err(RoleError::WrongArm);
        }
        self.header_scalar(2)
    }

    fn header_scalar(&self, tag: u32) -> Result<u64, RoleError> {
        let child = self
            .view
            .message(self.view.root(), self.arm.tag())
            .ok_or(RoleError::InternalPolicy)?;
        if self.view.has_field(child, tag) {
            self.view
                .scalar(child, tag)
                .ok_or(RoleError::InternalPolicy)
        } else {
            Ok(0)
        }
    }

    /// Presence is inspected at the common invocation extraction gate; no
    /// present terminal contents are interpreted here.
    pub fn terminal_present(&self) -> Result<bool, RoleError> {
        if self.arm != ActiveArm::StreamEnd {
            return Err(RoleError::WrongArm);
        }
        let child = self
            .view
            .message(self.view.root(), 9)
            .ok_or(RoleError::InternalPolicy)?;
        Ok(self.view.oneof_tag(child).is_some())
    }

    /// Span of the final arm's opaque bytes, if present. An absent ordinary
    /// proto3 bytes field has no span; a present oneof empty result does.
    pub fn opaque_payload_span(&self) -> Option<BodySpan<'body>> {
        let tag = self.arm.tag();
        let arm = self.view.message(self.view.root(), tag)?;
        let payload_tag = match self.arm {
            ActiveArm::Request => 8,
            ActiveArm::Response | ActiveArm::StreamEnd => 2,
            ActiveArm::StreamItem => 3,
            ActiveArm::Cancel | ActiveArm::Credit | ActiveArm::GoAway => return None,
        };
        self.view.bytes_span(arm, payload_tag)
    }

    /// Consume a selected StreamItem and retain only its checked original-body
    /// payload span. An absent ordinary proto3 bytes field is an empty payload.
    pub fn into_progress_payload(self) -> Result<ProgressPayload<'body>, RoleError> {
        if self.arm != ActiveArm::StreamItem {
            return Err(RoleError::WrongArm);
        }
        let item = self
            .view
            .message(self.view.root(), 8)
            .ok_or(RoleError::InternalPolicy)?;
        let span = if self.view.has_field(item, 3) {
            Some(
                self.view
                    .bytes_span(item, 3)
                    .ok_or(RoleError::InternalPolicy)?,
            )
        } else {
            None
        };
        Ok(ProgressPayload {
            span,
            body_len: self.body_len(),
        })
    }

    /// Consume a selected StreamEnd after checking the effective terminal
    /// oneof. A present empty result remains a result payload.
    pub fn into_result_terminal(self) -> Result<ResultTerminal<'view, 'body>, RoleError> {
        if self.arm != ActiveArm::StreamEnd {
            return Err(RoleError::WrongArm);
        }
        let end = self
            .view
            .message(self.view.root(), 9)
            .ok_or(RoleError::InternalPolicy)?;
        match self.view.oneof_tag(end) {
            Some(2) => Ok(ResultTerminal::Result(ResultPayload {
                span: self
                    .view
                    .bytes_span(end, 2)
                    .ok_or(RoleError::InternalPolicy)?,
                body_len: self.body_len(),
            })),
            Some(3) => {
                let error = self.view.message(end, 3).ok_or(RoleError::InternalPolicy)?;
                Ok(ResultTerminal::ErrorTerminal(ErrorTerminal {
                    body_len: self.body_len(),
                    view: self.view,
                    error,
                }))
            }
            None => Err(RoleError::InvalidTerminal),
            _ => Err(RoleError::InternalPolicy),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RoleError {
    WrongArm,
    InvalidTerminal,
    InternalPolicy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionError {
    InvalidProgress,
    InvalidTerminal,
    UnsupportedVariant,
    LocalResource(ScanError),
    InternalPolicy,
    Value(value::Error),
}

fn scan_error(error: ScanError, progress: bool) -> AdmissionError {
    match error {
        ScanError::Malformed if progress => AdmissionError::InvalidProgress,
        ScanError::Malformed => AdmissionError::InvalidTerminal,
        ScanError::UnsupportedVariant => AdmissionError::UnsupportedVariant,
        ScanError::Limit | ScanError::Allocation => AdmissionError::LocalResource(error),
    }
}

pub struct ProgressPayload<'body> {
    span: Option<BodySpan<'body>>,
    body_len: usize,
}

pub struct ResultPayload<'body> {
    span: BodySpan<'body>,
    body_len: usize,
}

pub enum ResultTerminal<'view, 'body> {
    Result(ResultPayload<'body>),
    ErrorTerminal(ErrorTerminal<'view, 'body>),
}

/// The effective merged error child stays within its owning scan. Peer text
/// is supplied only during synchronous validation and never enters diagnostics.
pub struct ErrorTerminal<'view, 'body> {
    view: IndexedMessage<'view, 'body>,
    error: MessageId,
    body_len: usize,
}

impl ErrorTerminal<'_, '_> {
    pub fn body_len(&self) -> usize {
        self.body_len
    }

    pub fn validate<T>(self, check: impl FnOnce(i32, &str) -> T) -> Result<T, RoleError> {
        let code = if self.view.has_field(self.error, 1) {
            (self
                .view
                .scalar(self.error, 1)
                .ok_or(RoleError::InternalPolicy)? as u32) as i32
        } else {
            0
        };
        let message = if self.view.has_field(self.error, 2) {
            self.view
                .text(self.error, 2)
                .ok_or(RoleError::InternalPolicy)?
        } else {
            ""
        };
        Ok(check(code, message))
    }
}

pub struct AdmittedProgress {
    pub json: Vec<u8>,
    pub percent_complete: Option<u32>,
    pub body_len: usize,
}

pub struct AdmittedResult {
    pub json: Vec<u8>,
    pub body_len: usize,
}

impl ProgressPayload<'_> {
    pub fn body_len(&self) -> usize {
        self.body_len
    }

    pub fn admit(
        self,
        arena: &mut BorrowedIndexArena<'_>,
        tool_minor: SelectedMinor,
        expected: &Projection<'_>,
        value_limits: value::Limits,
        instance_limits: jcs::Limits,
    ) -> Result<AdmittedProgress, AdmissionError> {
        let payload = match self.span {
            Some(span) => span.reborrow().ok_or(AdmissionError::InternalPolicy)?,
            None => &[],
        };
        let view = arena
            .scan_tool_progress(payload)
            .map_err(|error| scan_error(error, true))?;
        let percent_complete = if view.has_field(view.root(), 2) {
            if tool_minor == SelectedMinor::V1_0 {
                return Err(AdmissionError::UnsupportedVariant);
            }
            let percent = view
                .scalar(view.root(), 2)
                .ok_or(AdmissionError::InternalPolicy)? as u32;
            if percent > 100 {
                return Err(AdmissionError::InvalidProgress);
            }
            Some(percent)
        } else {
            None
        };
        let event = view
            .message(view.root(), 1)
            .ok_or(AdmissionError::InvalidProgress)?;
        let json = value::admit_indexed_schema_bound(
            &view,
            event,
            tool_minor,
            expected,
            value_limits,
            instance_limits,
        )
        .map_err(AdmissionError::Value)?;
        Ok(AdmittedProgress {
            json,
            percent_complete,
            body_len: self.body_len,
        })
    }
}

impl ResultPayload<'_> {
    pub fn body_len(&self) -> usize {
        self.body_len
    }

    pub fn admit(
        self,
        arena: &mut BorrowedIndexArena<'_>,
        tool_minor: SelectedMinor,
        expected: &Projection<'_>,
        value_limits: value::Limits,
        instance_limits: jcs::Limits,
    ) -> Result<AdmittedResult, AdmissionError> {
        let payload = self.span.reborrow().ok_or(AdmissionError::InternalPolicy)?;
        let view = arena
            .scan_tool_result(payload)
            .map_err(|error| scan_error(error, false))?;
        let result = view
            .message(view.root(), 1)
            .ok_or(AdmissionError::InvalidTerminal)?;
        let json = value::admit_indexed_schema_bound(
            &view,
            result,
            tool_minor,
            expected,
            value_limits,
            instance_limits,
        )
        .map_err(AdmissionError::Value)?;
        Ok(AdmittedResult {
            json,
            body_len: self.body_len,
        })
    }
}

/// Select after the scanner has validated every known field in wire order.
/// Active pre-Ready arms fail before the selected-1.0 reencoding limit, as in
/// Endpoint. All seven final active arms receive the same size guard.
pub fn select_active<'view, 'body>(
    mut view: IndexedMessage<'view, 'body>,
    core_minor: SelectedMinor,
    post_limit: NonZeroUsize,
) -> Result<SelectedActive<'view, 'body>, SelectError> {
    let arm = view.arm().ok_or(SelectError::InternalPolicy)?;
    let arm = ActiveArm::from_tag(arm).ok_or(SelectError::UnexpectedFrame)?;
    let normalized_len = if core_minor == SelectedMinor::V1_0 {
        let size = view.normalized_active_len().map_err(|error| match error {
            SizeError::UnmodeledField | SizeError::Overflow => SelectError::InternalPolicy,
        })?;
        if size > post_limit.get() {
            return Err(SelectError::FrameTooLarge {
                length: u64::try_from(size).unwrap_or(u64::MAX),
                limit: post_limit.get(),
            });
        }
        Some(size)
    } else {
        None
    };
    Ok(SelectedActive {
        view,
        arm,
        normalized_len,
    })
}

#[cfg(test)]
pub(crate) fn exhausted_scan_fixture() -> ScanError {
    use std::sync::atomic::AtomicU64;
    super::echo_bounded::next_scan_token_from(&AtomicU64::new(u64::MAX)).unwrap_err()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    #[test]
    fn exhausted_scan_identity_maps_to_local_resource() {
        let counter = AtomicU64::new(u64::MAX);
        let error = super::super::echo_bounded::next_scan_token_from(&counter).unwrap_err();
        assert_eq!(
            scan_error(error, true),
            AdmissionError::LocalResource(ScanError::Limit)
        );
        assert_eq!(
            scan_error(error, false),
            AdmissionError::LocalResource(ScanError::Limit)
        );
    }
}
