use std::future::Future;

use nix::sys::signal::Signal;
use tokio::sync::{mpsc, oneshot, watch};
use tonic::Status;

use crate::pb::bracket::portproxy::v1::{ExecControlRequest, exec_control_request};
use crate::process_group::signal_process_group_or_pid;

type SignalRequest = (
    Option<Signal>,
    (u64, u64),
    oneshot::Sender<Result<(), Status>>,
);

#[derive(Clone)]
pub struct ExecControl {
    signals: mpsc::Sender<SignalRequest>,
    eof: watch::Sender<bool>,
}

pub struct ExecControlReceiver {
    signals: mpsc::Receiver<SignalRequest>,
    pub eof: watch::Receiver<bool>,
    last_signal: Option<(u64, u64)>,
    terminating: bool,
}

pub fn channel() -> (ExecControl, ExecControlReceiver) {
    let (signals, receiver) = mpsc::channel(8);
    let (eof, eof_receiver) = watch::channel(false);
    (
        ExecControl { signals, eof },
        ExecControlReceiver {
            signals: receiver,
            eof: eof_receiver,
            last_signal: None,
            terminating: false,
        },
    )
}

impl ExecControl {
    pub async fn apply(&self, request: ExecControlRequest) -> Result<(), Status> {
        match request.control {
            Some(exec_control_request::Control::Claim(true)) => {
                let (sender, receiver) = oneshot::channel();
                self.signals
                    .send((None, (request.producer_epoch, 0), sender))
                    .await
                    .map_err(|_| Status::failed_precondition("execution has exited"))?;
                receiver
                    .await
                    .map_err(|_| Status::failed_precondition("execution has exited"))?
            }
            Some(exec_control_request::Control::StdinEof(true)) => {
                self.eof.send_replace(true);
                Ok(())
            }
            Some(exec_control_request::Control::Signal(number)) => {
                let signal = Signal::try_from(number)
                    .map_err(|_| Status::invalid_argument("invalid Unix signal"))?;
                let (sender, receiver) = oneshot::channel();
                if request.control_seq == 0 {
                    return Err(Status::invalid_argument("control_seq must be positive"));
                }
                self.signals
                    .send((
                        Some(signal),
                        (request.producer_epoch, request.control_seq),
                        sender,
                    ))
                    .await
                    .map_err(|_| Status::failed_precondition("execution has exited"))?;
                receiver
                    .await
                    .map_err(|_| Status::failed_precondition("execution has exited"))?
            }
            _ => Err(Status::invalid_argument(
                "signal or stdin_eof=true is required",
            )),
        }
    }
}

impl ExecControlReceiver {
    pub async fn wait<T>(&mut self, pid: i32, completion: impl Future<Output = T>) -> T {
        tokio::pin!(completion);
        let mut controls_open = true;
        loop {
            tokio::select! {
                biased;
                result = &mut completion => {
                    if self.terminating {
                        let _ = nix::sys::signal::kill(nix::unistd::Pid::from_raw(-pid), Signal::SIGKILL);
                    }
                    self.signals.close();
                    while self.signals.try_recv().is_ok() {}
                    return result;
                },
                request = self.signals.recv(), if controls_open => {
                    if let Some((signal, identity, reply)) = request {
                        if signal.is_none() {
                            let result = match self.last_signal {
                                Some(last) if identity.0 < last.0 => Err(Status::failed_precondition("stale execution owner")),
                                Some(last) if identity.0 == last.0 => Ok(()),
                                _ => { self.last_signal = Some(identity); Ok(()) },
                            };
                            let _ = reply.send(result);
                            continue;
                        }
                        let signal = signal.expect("signal checked above");
                        let result = match self.last_signal {
                            Some(last) if identity == last => Ok(()),
                            Some(last) if identity < last => Err(Status::failed_precondition("stale execution control")),
                            _ => signal_process_group_or_pid(pid, signal)
                                .map_err(|error| Status::internal(format!("signal failed: {error}"))),
                        };
                        if result.is_ok() {
                            self.last_signal = Some(identity);
                            self.terminating |= matches!(signal, Signal::SIGTERM | Signal::SIGINT | Signal::SIGHUP | Signal::SIGQUIT | Signal::SIGKILL);
                        }
                        let _ = reply.send(result);
                    } else {
                        controls_open = false;
                    }
                }
            }
        }
    }
}

pub async fn stdin_closed(receiver: &mut watch::Receiver<bool>) {
    loop {
        if *receiver.borrow_and_update() {
            return;
        }
        if receiver.changed().await.is_err() {
            return;
        }
    }
}
