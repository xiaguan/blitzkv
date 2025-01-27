#![allow(unused)]

use std::{convert::TryFrom, io};

use libc::{c_int, c_long, c_uint, syscall};

use super::io_uring_params;

const SETUP: c_long = 425;
const ENTER: c_long = 426;
const REGISTER: c_long = 427;

pub(crate) fn setup(
    entries: c_uint,
    p: *mut io_uring_params,
) -> io::Result<c_int> {
    assert!(
        (1..=4096).contains(&entries),
        "entries must be between 1 and 4096 (inclusive)"
    );
    assert_eq!(
        entries.count_ones(),
        1,
        "entries must be a power of 2"
    );
    #[allow(unsafe_code)]
    let ret = unsafe {
        syscall(SETUP, i64::from(entries), p as c_long)
    };
    if ret < 0 {
        let mut err = io::Error::last_os_error();
        if let Some(12) = err.raw_os_error() {
            err = io::Error::new(
                io::ErrorKind::Other,
                "Not enough lockable memory. You probably \
                 need to raise the memlock rlimit, which \
                 often defaults to a pretty low number.",
            );
        }
        return Err(err);
    }
    Ok(i32::try_from(ret).unwrap())
}

pub(crate) fn enter(
    fd: c_int,
    to_submit: c_uint,
    min_complete: c_uint,
    flags: c_uint,
    sig: *mut libc::sigset_t,
) -> io::Result<c_int> {
    loop {
        // this is strapped into an interruption
        // diaper loop because it's the one that
        // might actually block a lot
        #[allow(unsafe_code)]
        let ret = unsafe {
            syscall(
                ENTER,
                i64::from(fd),
                i64::from(to_submit),
                i64::from(min_complete),
                i64::from(flags),
                sig as c_long,
                size_of::<libc::sigset_t>() as c_long,
            )
        };
        if ret < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        } else {
            return Ok(i32::try_from(ret).unwrap());
        }
    }
}

pub(crate) fn register(
    fd: c_int,
    opcode: c_uint,
    arg: *const libc::c_void,
    nr_args: c_uint,
) -> io::Result<c_int> {
    #[allow(unsafe_code)]
    let ret = unsafe {
        syscall(
            REGISTER,
            i64::from(fd),
            i64::from(opcode),
            arg as c_long,
            i64::from(nr_args),
        )
    };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(i32::try_from(ret).unwrap())
}
