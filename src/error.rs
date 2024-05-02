use core::fmt;

use http::StatusCode;

#[derive(Debug)]
pub enum ErrorKind {
    Internal,
    Profile,
    Request,
    ClientError { status: StatusCode, code: String },
    ServerError { status: StatusCode, code: String },
    ParseResponse,
}

#[derive(Debug)]
pub struct DeltaSharingError {
    kind: ErrorKind,
    message: String,
}

impl DeltaSharingError {
    pub fn new(kind: ErrorKind, message: String) -> Self {
        Self { kind, message }
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }

    pub fn is_not_found(&self) -> bool {
        matches!(
            self.kind,
            ErrorKind::ClientError {
                status: StatusCode::NOT_FOUND,
                ..
            }
        )
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Internal, message.into())
    }

    pub fn request(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Request, message.into())
    }

    pub fn profile(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Profile, message.into())
    }

    pub fn client(status: StatusCode, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(
            ErrorKind::ClientError {
                status,
                code: code.into(),
            },
            message.into(),
        )
    }

    pub fn server(status: StatusCode, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(
            ErrorKind::ServerError {
                status,
                code: code.into(),
            },
            message.into(),
        )
    }

    pub fn parse_response(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::ParseResponse, message.into())
    }
}

impl fmt::Display for DeltaSharingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ErrorKind::ClientError { status, code } => {
                write!(f, "Client error: {} - {} - {}", status, code, self.message)
            }
            ErrorKind::ServerError { status, code } => {
                write!(f, "Server error: {} - {} - {}", status, code, self.message)
            }
            ErrorKind::Internal => todo!(),
            ErrorKind::Profile => todo!(),
            ErrorKind::ParseResponse => todo!(),
            ErrorKind::Request => todo!(),
        }
    }
}

impl std::error::Error for DeltaSharingError {}
