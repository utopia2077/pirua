//! Private protocol implementation for the companion Rust helper executable.
use crate::{protocol::*, types::*};
use rquickjs::{
    Context, Ctx, Exception, Function, Object, Runtime, Value as JsValue, context::intrinsic,
};
use serde_json::Value;
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    io::{self, BufReader},
    rc::Rc,
};

const PRELUDE: &str = include_str!("prelude.js");
const CLOCK: &str = include_str!("clock.js");

fn fingerprint(start: &Start) -> io::Result<String> {
    #[cfg(target_os = "linux")]
    let binary = std::fs::read("/proc/self/exe")?;
    #[cfg(not(target_os = "linux"))]
    let binary = std::fs::read(std::env::current_exe()?)?;
    let digest = ring::digest::digest(&ring::digest::SHA256, &binary);
    let hash: String = digest.as_ref().iter().map(|v| format!("{v:02x}")).collect();
    Ok(format!(
        "pi-codemode/1 rquickjs/0.14.0 quickjs-ng/0.16.2 binary/{hash} {}/{} heap/{} stack/{} UTC",
        std::env::consts::OS,
        std::env::consts::ARCH,
        start.memory,
        start.stack
    ))
}
fn script_error(ctx: &Ctx<'_>, error: rquickjs::Error) -> ExecutionError {
    let value = ctx.catch();
    if let Some(exception) = value.as_exception() {
        return ExecutionError {
            kind: ErrorKind::Script,
            name: exception.as_object().get("name").ok(),
            message: exception.message().unwrap_or_else(|| error.to_string()),
            stack: exception.stack(),
        };
    }
    ExecutionError {
        kind: ErrorKind::Script,
        name: None,
        message: error.to_string(),
        stack: None,
    }
}
fn parse_json(ctx: &Ctx<'_>, text: Option<String>) -> rquickjs::Result<Option<Value>> {
    text.map(|text| crate::json::value(&text).map_err(|error| Exception::throw_type(ctx, &error.0)))
        .transpose()
}
fn writes(ctx: &Ctx<'_>, text: &str) -> rquickjs::Result<StoreWrites> {
    let entries: Vec<Vec<String>> = crate::json::decode(text.as_bytes(), MAX_JSON_DEPTH + 8)
        .map_err(|error| Exception::throw_type(ctx, &error.0))?;
    let mut result = StoreWrites::default();
    for entry in entries {
        if let Some(key) = entry.first() {
            if let Some(json) = entry.get(1) {
                result.set.insert(
                    key.clone(),
                    crate::json::value(json)
                        .map_err(|error| Exception::throw_type(ctx, &error.0))?,
                );
            } else {
                result.delete.push(key.clone());
            }
        }
    }
    Ok(result)
}

/// Run the internal helper protocol. Hosts launch the supplied binary rather
/// than calling this entry point in their own process.
#[doc(hidden)]
pub fn worker_main() -> Result<(), Box<dyn std::error::Error>> {
    let mut input = BufReader::new(io::stdin().lock());
    let start: Start = read_json(&mut input)?.ok_or("missing CodeMode start frame")?;
    let fingerprint = fingerprint(&start)?;
    let runtime = Runtime::new()?;
    runtime.set_memory_limit(start.memory);
    runtime.set_max_stack_size(start.stack);
    // No WeakRef, native performance, module loader, I/O, timers or host APIs.
    let context = Context::builder()
        .with::<(
            intrinsic::Date,
            intrinsic::Eval,
            intrinsic::RegExp,
            intrinsic::Json,
            intrinsic::Proxy,
            intrinsic::MapSet,
            intrinsic::TypedArrays,
            intrinsic::Promise,
        )>()
        .build(&runtime)?;
    let output = Rc::new(RefCell::new(io::stdout().lock()));
    let finished = Rc::new(Cell::new(false));
    let issued = Rc::new(Cell::new(0u64));
    let bridge_bytes = Rc::new(Cell::new(0usize));
    let current = Rc::new(Cell::new(0u64));
    let result = context.with(|ctx| -> rquickjs::Result<()> {
        let emit = {
            let output = output.clone();
            move |message: &Message| -> io::Result<()> {
                write_json(&mut *output.borrow_mut(), message)
            }
        };
        let bridge = {
            let finished = finished.clone();
            let output = output.clone();
            let issued = issued.clone();
            let bridge_bytes = bridge_bytes.clone();
            let current = current.clone();
            let controlled = start.controlled;
            Function::new(
                ctx.clone(),
                move |ctx: Ctx<'_>,
                      kind: String,
                      a: JsValue<'_>,
                      b: Option<String>,
                      c: Option<String>|
                      -> rquickjs::Result<()> {
                    if finished.get() {
                        return Ok(());
                    }
                    if kind == "validate" {
                        let json = a.as_string().ok_or(rquickjs::Error::Unknown)?.to_string()?;
                        crate::json::value(&json)
                            .map_err(|error| Exception::throw_type(&ctx, &error.0))?;
                        return Ok(());
                    }
                    let mut admitted = None;
                    let terminal = kind == "done";
                    let message = match kind.as_str() {
                        "call" | "global" => {
                            let id = a.as_number().unwrap_or_default() as u64;
                            let json_bytes = c.as_ref().map_or(0, String::len);
                            if id > MAX_CALLS as u64
                                || json_bytes > MAX_JSON_BYTES
                                || bridge_bytes.get() + json_bytes > MAX_BRIDGE_BYTES
                            {
                                let error = ExecutionError {
                                    kind: ErrorKind::Script,
                                    name: Some("RangeError".into()),
                                    message: "CodeMode call bridge limit exceeded".into(),
                                    stack: None,
                                };
                                write_json(
                                    &mut *output.borrow_mut(),
                                    &Message::Failed {
                                        error,
                                        during: controlled.then_some(During {
                                            seq: current.get(),
                                            issued: issued.get(),
                                        }),
                                    },
                                )
                                .map_err(|_| rquickjs::Error::Unknown)?;
                                finished.set(true);
                                return Err(Exception::throw_range(
                                    &ctx,
                                    "CodeMode call bridge limit exceeded",
                                ));
                            }
                            let args = parse_json(&ctx, c)?;
                            admitted = Some((id, json_bytes));
                            Message::Call {
                                call: HostCall {
                                    id,
                                    target: if kind == "call" {
                                        CallTarget::Tool
                                    } else {
                                        CallTarget::Global
                                    },
                                    name: b.unwrap_or_default(),
                                    args,
                                },
                            }
                        }
                        "output" => {
                            let output_kind =
                                a.as_string().ok_or(rquickjs::Error::Unknown)?.to_string()?;
                            Message::Output {
                                item: if output_kind == "image" {
                                    OutputItem::Image {
                                        data: b.unwrap_or_default(),
                                        mime_type: c.unwrap_or_default(),
                                    }
                                } else {
                                    OutputItem::Text {
                                        text: b.unwrap_or_default(),
                                        console: output_kind == "console",
                                    }
                                },
                            }
                        }
                        "done" => {
                            if a.as_bool().unwrap_or(false) {
                                Message::Done {
                                    value: parse_json(&ctx, b)?,
                                    writes: writes(&ctx, &c.unwrap_or_default())?,
                                    during: controlled.then_some(During {
                                        seq: current.get(),
                                        issued: issued.get(),
                                    }),
                                }
                            } else {
                                let value: Value = parse_json(&ctx, b)?.unwrap_or(Value::Null);
                                Message::Failed {
                                    error: ExecutionError {
                                        kind: ErrorKind::Script,
                                        name: value
                                            .get("name")
                                            .and_then(Value::as_str)
                                            .map(str::to_owned),
                                        message: value
                                            .get("message")
                                            .and_then(Value::as_str)
                                            .unwrap_or("Script failed")
                                            .to_owned(),
                                        stack: value
                                            .get("stack")
                                            .and_then(Value::as_str)
                                            .map(str::to_owned),
                                    },
                                    during: controlled.then_some(During {
                                        seq: current.get(),
                                        issued: issued.get(),
                                    }),
                                }
                            }
                        }
                        _ => return Err(rquickjs::Error::Unknown),
                    };
                    write_json(&mut *output.borrow_mut(), &message)
                        .map_err(|_| rquickjs::Error::Unknown)?;
                    if let Some((id, bytes)) = admitted {
                        bridge_bytes.set(bridge_bytes.get() + bytes);
                        issued.set(id);
                    }
                    if terminal {
                        finished.set(true);
                    }
                    Ok(())
                },
            )?
        };
        let clock_factory: Function = ctx.eval(CLOCK)?;
        let clock: Function =
            clock_factory.call((start.started_at, start.seed, start.controlled))?;
        let factory: Function = ctx.eval(PRELUDE)?;
        let api: Object = factory.call((
            bridge,
            serde_json::to_string(&start.tools).unwrap(),
            serde_json::to_string(&start.globals).unwrap(),
            serde_json::to_string(&start.store).unwrap(),
        ))?;
        emit(&Message::Started { fingerprint }).map_err(|_| rquickjs::Error::Unknown)?;
        let run: Function = api.get("run")?;
        let settle: Function = api.get("settle")?;
        let stalled: Function = api.get("stalled")?;
        let function: Function =
            ctx.eval(format!("(async (tools, console) => {{{}\n}})", start.code))?;
        run.call::<_, ()>((function,))?;
        let drain = || -> rquickjs::Result<()> {
            while !finished.get() && ctx.execute_pending_job() {}
            if !finished.get() {
                stalled.call::<_, ()>(())?;
            }
            Ok(())
        };
        drain()?;
        if !finished.get() {
            emit(&Message::Idle {
                seq: 0,
                issued: issued.get(),
            })
            .map_err(|_| rquickjs::Error::Unknown)?;
        }
        let mut next_seq = 1;
        let mut early = BTreeMap::new();
        while !finished.get() {
            let delivery: Delivery = read_json(&mut input)
                .map_err(|_| rquickjs::Error::Unknown)?
                .ok_or(rquickjs::Error::Unknown)?;
            early.insert(delivery.seq, delivery);
            while let Some(delivery) = early.remove(&next_seq) {
                current.set(delivery.seq);
                clock.call::<_, ()>((delivery.now,))?;
                let (ok, payload, properties) = match delivery.outcome {
                    Outcome::Success { value } => (
                        true,
                        value.map(|v| serde_json::to_string(&v).unwrap()),
                        None,
                    ),
                    Outcome::Error {
                        message,
                        properties,
                    } => (
                        false,
                        Some(message),
                        Some(serde_json::to_string(&properties).unwrap()),
                    ),
                };
                settle.call::<_, ()>((delivery.id, ok, payload, properties))?;
                drain()?;
                if finished.get() {
                    break;
                }
                emit(&Message::Idle {
                    seq: next_seq,
                    issued: issued.get(),
                })
                .map_err(|_| rquickjs::Error::Unknown)?;
                next_seq += 1;
            }
        }
        Ok(())
    });
    if let Err(error) = result
        && !finished.get()
    {
        let error = context.with(|ctx| script_error(&ctx, error));
        write_json(
            &mut *output.borrow_mut(),
            &Message::Failed {
                error,
                during: start.controlled.then_some(During {
                    seq: current.get(),
                    issued: issued.get(),
                }),
            },
        )?;
    }
    Ok(())
}
