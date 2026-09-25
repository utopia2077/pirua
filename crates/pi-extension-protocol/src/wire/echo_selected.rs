//! Inert selected-core guard and borrowed active Envelope view.
//!
//! Its input is a successful full v1.1 Envelope scan. Endpoint and the live
//! pump still use Prost; this module does not admit tool values or debit credit.

use std::num::NonZeroUsize;

use super::echo_bounded::{BodySpan, IndexedMessage, SelectedMinor, SizeError};

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
