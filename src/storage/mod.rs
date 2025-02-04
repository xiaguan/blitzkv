mod completion;
pub mod device;
mod histogram;
pub mod io_uring;
mod lazy;
mod metrics;
pub mod page;

/// Create a new IO system.
pub fn new() -> std::io::Result<Rio> {
    Config::default().start()
}

/// Encompasses various types of IO structures that
/// can be operated on as if they were a libc::iovec
pub trait AsIoVec {
    /// Returns the address of this object.
    fn into_new_iovec(&self) -> libc::iovec;
}

impl<A: ?Sized + AsRef<[u8]>> AsIoVec for A {
    fn into_new_iovec(&self) -> libc::iovec {
        let self_ref: &[u8] = self.as_ref();
        let self_ptr: *const [u8] = self_ref;
        libc::iovec {
            iov_base: self_ptr as *mut _,
            iov_len: self_ref.len(),
        }
    }
}

pub trait AsIoVecMut {}

impl<A: ?Sized + AsMut<[u8]>> AsIoVecMut for A {}

/// A trait for describing transformations from the
/// `io_uring_cqe` type into an expected meaningful
/// high-level result.
pub trait FromCqe {
    /// Describes a conversion from a successful
    /// `io_uring_cqe` to a desired output type.
    fn from_cqe(cqe: io_uring::io_uring_cqe) -> Self;
}

impl FromCqe for usize {
    fn from_cqe(cqe: io_uring::io_uring_cqe) -> usize {
        use std::convert::TryFrom;
        usize::try_from(cqe.res).unwrap()
    }
}

impl FromCqe for () {
    fn from_cqe(_: io_uring::io_uring_cqe) {}
}

use io_uring::{Config, Rio};
