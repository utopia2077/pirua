use pi_codemode::*;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

fn sandbox() -> Sandbox {
    Sandbox::new(SandboxOptions::new(env!(
        "CARGO_BIN_EXE_pi-codemode-worker"
    )))
    .unwrap()
}
fn options() -> RunOptions {
    let mut o = RunOptions::new(1_768_471_200_000.0, 1);
    o.timeout = Some(Duration::from_secs(5));
    o
}
fn event(run: &Run) -> Event {
    run.next_event(Duration::from_secs(5))
        .expect("expected VM event")
}
fn call(run: &Run) -> HostCall {
    loop {
        if let Event::Call(call) = event(run) {
            return call;
        }
    }
}
fn value(result: ExecutionResult) -> Value {
    match result {
        ExecutionResult::Success { value, .. } => value.unwrap_or(Value::Null),
        other => panic!("expected success: {other:?}"),
    }
}

#[test]
fn actual_vm_success_output_and_image_tool() {
    let sandbox = sandbox();
    let result = sandbox
        .execute(
            include_str!("fixtures/output.js"),
            ExecuteOptions {
                tools: vec![DeclaredTool::new("mcp/read-image")],
                ..Default::default()
            },
            |call, _| {
                assert_eq!(call.name, "mcp/read-image");
                assert_eq!(call.args, Some(json!({"path":"example.png"})));
                Outcome::success(
                    json!({"type":"image","data":"iVBORw0KGgo=","mimeType":"image/png"}),
                )
            },
        )
        .unwrap();
    match result {
        ExecutionResult::Success {
            value,
            output,
            calls,
            during,
            ..
        } => {
            assert_eq!(value, Some(json!(["mcp_read_image"])));
            assert_eq!(during, None);
            assert_eq!(
                output,
                vec![
                    OutputItem::Text {
                        text: "hello 1 {\"a\":true}".into(),
                        console: true
                    },
                    OutputItem::Text {
                        text: "{\"count\":2}".into(),
                        console: false
                    },
                    OutputItem::Image {
                        data: "iVBORw0KGgo=".into(),
                        mime_type: "image/png".into()
                    }
                ]
            );
            assert_eq!(calls[0].status, CallStatus::Ok);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(sandbox.active_workers(), 0);
}
#[test]
fn script_error_and_stalled_promise_keep_output_but_no_writes() {
    for code in [
        "store('bad',1); text('before'); throw new Error('boom')",
        "text('before'); await new Promise(() => {})",
        "this is invalid syntax !",
    ] {
        let result = sandbox()
            .execute(code, ExecuteOptions::default(), |_, _| unreachable!())
            .unwrap();
        assert!(
            matches!(
                result,
                ExecutionResult::Error {
                    error: ExecutionError {
                        kind: ErrorKind::Script,
                        ..
                    },
                    ..
                }
            ),
            "{result:?}"
        );
        if !code.contains("invalid syntax") {
            assert!(matches!(result, ExecutionResult::Error { output, .. }
                if output == vec![OutputItem::Text { text: "before".into(), console: false }]));
        }
    }
}
#[test]
fn tool_error_properties_and_spread_globals() {
    let mut o = options();
    o.tools.push(DeclaredTool::new("echo"));
    o.globals.push(DeclaredGlobal {
        name: "catalog.search".into(),
        spread: true,
    });
    let run = sandbox().start("const x=await catalog.search('a',2); try{await tools.echo(x)}catch(e){return [e instanceof Error,e.message,e.name,e.structuredOutput]}", o).unwrap();
    assert!(matches!(event(&run), Event::Started { .. }));
    let a = call(&run);
    assert_eq!(a.target, CallTarget::Global);
    assert_eq!(a.args, Some(json!(["a", 2])));
    run.deliver(1, 1.0, a.id, Outcome::success(json!(7)))
        .unwrap();
    let b = call(&run);
    run.deliver(
        2,
        2.0,
        b.id,
        Outcome::Error {
            message: "denied".into(),
            properties: [
                ("structuredOutput".into(), json!({"code":7})),
                ("name".into(), json!("Ignored")),
            ]
            .into(),
        },
    )
    .unwrap();
    assert_eq!(value(run.wait()), json!([true,"denied","Error",{"code":7}]));
}
#[test]
fn ordered_deliveries_replay_clock_seed_and_idle_counts() {
    let mut o = options();
    o.tools.push(DeclaredTool::new("wait"));
    let code = include_str!("fixtures/replay.js");
    let mut recordings = Vec::new();
    for _ in 0..2 {
        let run = sandbox().start(code, o.clone()).unwrap();
        let started = event(&run);
        let a = call(&run);
        let b = call(&run);
        assert_eq!(event(&run), Event::Idle { seq: 0, issued: 2 });
        run.deliver(2, o.started_at + 2000.0, a.id, Outcome::success(json!("A")))
            .unwrap();
        assert_eq!(run.next_event(Duration::from_millis(20)), None);
        run.deliver(1, o.started_at + 1000.0, b.id, Outcome::success(json!("B")))
            .unwrap();
        assert_eq!(event(&run), Event::Idle { seq: 1, issued: 2 });
        let result = run.wait();
        assert!(
            matches!(
                &result,
                ExecutionResult::Success {
                    during: Some(During { seq: 2, issued: 2 }),
                    ..
                }
            ),
            "{result:?}"
        );
        let result_value = value(result);
        assert_eq!(
            result_value[0],
            json!([
                o.started_at as i64,
                0,
                0.6270739405881613,
                "2026-01-15T10:00:00.000Z",
                0,
                0
            ])
        );
        assert_eq!(result_value[1], json!(["B", "A"]));
        assert_eq!(
            result_value[2],
            json!([(o.started_at + 2000.0) as i64, 2000])
        );
        recordings.push((started, a, b, result_value));
    }
    assert_eq!(recordings[0], recordings[1]);
}
#[test]
fn clock_monotonic_and_delivery_validation_is_atomic() {
    let mut o = options();
    o.tools.push(DeclaredTool::new("wait"));
    let run = sandbox()
        .start(
            "await tools.wait();return [Date.now(),performance.now()]",
            o.clone(),
        )
        .unwrap();
    let c = call(&run);
    assert_eq!(c.args, None);
    assert!(
        run.deliver(1, f64::NAN, c.id, Outcome::undefined())
            .is_err()
    );
    assert!(run.deliver(0, 1.0, c.id, Outcome::undefined()).is_err());
    assert!(run.deliver(1, 1.0, 99, Outcome::undefined()).is_err());
    assert!(
        run.deliver(
            1,
            1.0,
            c.id,
            Outcome::success(json!("x".repeat(MAX_JSON_BYTES)))
        )
        .is_err()
    );
    run.deliver(1, o.started_at - 100.0, c.id, Outcome::undefined())
        .unwrap();
    assert_eq!(value(run.wait()), json!([o.started_at as i64, 0]));
    run.deliver(1, 1.0, c.id, Outcome::undefined()).unwrap();
}
#[test]
fn store_is_detached_success_only_and_exit_is_success() {
    let mut o = ExecuteOptions::default();
    o.store.insert("old".into(), json!({"nested":[1]}));
    let result = sandbox()
        .execute(include_str!("fixtures/store.js"), o, |_, _| unreachable!())
        .unwrap();
    match result {
        ExecutionResult::Success {
            value,
            store_writes,
            output,
            ..
        } => {
            assert_eq!(value, None);
            assert_eq!(store_writes.set.get("new"), Some(&json!({"nested":[1,2]})));
            assert_eq!(store_writes.delete, vec!["old"]);
            assert_eq!(
                output,
                vec![OutputItem::Text {
                    text: "1".into(),
                    console: false
                }]
            );
        }
        other => panic!("{other:?}"),
    }
}
#[test]
fn cancellation_deadline_and_close_reap_cpu_bound_children() {
    for code in [
        "while(true){}",
        "while(true) await null",
        "try{exit()}catch{} while(true){}",
    ] {
        let sandbox = sandbox();
        let mut o = options();
        o.timeout = Some(Duration::from_millis(250));
        let run = sandbox.start(code, o).unwrap();
        let pid = run.process_id();
        let began = Instant::now();
        let result = run.wait();
        if code.contains("exit") {
            assert!(result.is_ok());
        } else {
            assert!(matches!(
                result,
                ExecutionResult::Error {
                    error: ExecutionError {
                        kind: ErrorKind::Timeout,
                        ..
                    },
                    ..
                }
            ));
        }
        assert!(began.elapsed() < Duration::from_secs(3));
        assert_eq!(sandbox.active_workers(), 0);
        #[cfg(target_os = "linux")]
        assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    }
    let sandbox = sandbox();
    let run = sandbox.start("while(true){}", options()).unwrap();
    assert!(matches!(event(&run), Event::Started { .. }));
    run.cancel();
    assert!(matches!(
        run.wait(),
        ExecutionResult::Error {
            error: ExecutionError {
                kind: ErrorKind::Aborted,
                ..
            },
            ..
        }
    ));
    let run = sandbox.start("while(true){}", options()).unwrap();
    sandbox.close();
    assert!(matches!(
        run.wait(),
        ExecutionResult::Error {
            error: ExecutionError {
                kind: ErrorKind::Aborted,
                ..
            },
            ..
        }
    ));
    assert!(sandbox.start("return 1", options()).is_err());
}
#[test]
fn finishing_cancels_unawaited_host_tools() {
    let cancelled = Arc::new(AtomicBool::new(false));
    let observed = cancelled.clone();
    let result = sandbox()
        .execute(
            "tools.wait();await tools.echo();return 1",
            ExecuteOptions {
                tools: vec![DeclaredTool::new("wait"), DeclaredTool::new("echo")],
                ..Default::default()
            },
            move |call, token| {
                if call.name == "echo" {
                    return Outcome::undefined();
                }
                let deadline = Instant::now() + Duration::from_secs(3);
                while !token.is_cancelled() && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(1));
                }
                observed.store(token.is_cancelled(), Ordering::Release);
                Outcome::undefined()
            },
        )
        .unwrap();
    assert!(
        matches!(result, ExecutionResult::Success { ref calls, .. } if calls[0].status == CallStatus::Cancelled)
    );
    let deadline = Instant::now() + Duration::from_secs(3);
    while !cancelled.load(Ordering::Acquire) && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(cancelled.load(Ordering::Acquire));
}
#[test]
fn no_ambient_io_or_weakrefs_and_intrinsics_are_locked() {
    let result = sandbox()
        .execute(
            include_str!("fixtures/capabilities.js"),
            ExecuteOptions::default(),
            |_, _| unreachable!(),
        )
        .unwrap();
    assert_eq!(
        value(result),
        json!([
            "undefined",
            "undefined",
            "undefined",
            "undefined",
            "undefined",
            "undefined",
            "undefined",
            true,
            true,
            true
        ])
    );
}
#[test]
fn admission_is_held_until_exit_and_bad_declarations_fail_before_launch() {
    let mut config = SandboxOptions::new(env!("CARGO_BIN_EXE_pi-codemode-worker"));
    config.max_workers = 1;
    let sandbox = Sandbox::new(config).unwrap();
    let mut waiting = options();
    waiting.tools.push(DeclaredTool::new("wait"));
    let run = sandbox.start("await tools.wait()", waiting).unwrap();
    call(&run);
    assert!(sandbox.start("return 2", options()).is_err());
    run.cancel();
    run.wait();
    assert_eq!(
        value(sandbox.start("return 2", options()).unwrap().wait()),
        json!(2)
    );
    for names in [["a-b", "a_b"], ["foo", "foo"]] {
        let mut o = options();
        o.tools = names.into_iter().map(DeclaredTool::new).collect();
        assert!(sandbox.start("", o).is_err());
    }
    let mut o = options();
    o.globals.push(DeclaredGlobal {
        name: "console.log".into(),
        spread: false,
    });
    assert!(sandbox.start("", o).is_err());
    assert_eq!(sandbox.active_workers(), 0);
}
#[test]
fn output_store_and_memory_limits_are_enforced_in_the_vm() {
    for code in [
        "try{for(let i=0;i<100001;i++)text('')}catch{}return 'caught'",
        "text('x'.repeat(16*1024*1024+1))",
        "store('big','x'.repeat(256*1024));",
        "const a=[];while(true)a.push('x'.repeat(1024*1024));",
    ] {
        let result = sandbox()
            .execute(
                code,
                ExecuteOptions {
                    timeout: Some(Duration::from_secs(5)),
                    ..Default::default()
                },
                |_, _| unreachable!(),
            )
            .unwrap();
        assert!(
            matches!(
                result,
                ExecutionResult::Error {
                    error: ExecutionError {
                        kind: ErrorKind::Script,
                        ..
                    },
                    ..
                }
            ),
            "{result:?}"
        );
    }
}

#[test]
fn sequential_drains_report_new_calls_and_terminal_sequence() {
    let mut o = options();
    o.tools.push(DeclaredTool::new("echo"));
    let run = sandbox()
        .start(
            "await tools.echo(1);await Promise.all([tools.echo(2),tools.echo(3)]);return 9",
            o.clone(),
        )
        .unwrap();
    assert!(matches!(event(&run), Event::Started { .. }));
    assert_eq!(call(&run).id, 1);
    assert_eq!(event(&run), Event::Idle { seq: 0, issued: 1 });
    run.deliver(1, o.started_at, 1, Outcome::undefined())
        .unwrap();
    assert_eq!(call(&run).id, 2);
    assert_eq!(call(&run).id, 3);
    assert_eq!(event(&run), Event::Idle { seq: 1, issued: 3 });
    assert!(
        run.deliver(1, o.started_at, 2, Outcome::undefined())
            .is_err()
    );
    assert!(
        run.deliver(2, o.started_at, 1, Outcome::undefined())
            .is_err()
    );
    run.deliver(2, o.started_at, 2, Outcome::undefined())
        .unwrap();
    assert_eq!(event(&run), Event::Idle { seq: 2, issued: 3 });
    run.deliver(3, o.started_at, 3, Outcome::undefined())
        .unwrap();
    assert!(matches!(
        run.wait(),
        ExecutionResult::Success {
            during: Some(During { seq: 3, issued: 3 }),
            ..
        }
    ));
}
#[test]
fn caller_abort_while_waiting_is_not_a_catchable_tool_error() {
    let mut o = options();
    o.tools.push(DeclaredTool::new("wait"));
    let cancel = o.cancellation.clone();
    let run = sandbox()
        .start(
            "try{await tools.wait()}catch{text('caught')}text('after')",
            o,
        )
        .unwrap();
    call(&run);
    cancel.cancel();
    assert!(
        matches!(run.wait(),ExecutionResult::Error{error:ExecutionError{kind:ErrorKind::Aborted,..},output,..} if output.is_empty())
    );
}
#[test]
fn helper_environment_is_minimal_and_missing_or_crashed_helper_is_explicit() {
    let mut o = options();
    o.tools.push(DeclaredTool::new("wait"));
    let run = sandbox().start("await tools.wait()", o).unwrap();
    call(&run);
    #[cfg(target_os = "linux")]
    {
        let bytes = std::fs::read(format!("/proc/{}/environ", run.process_id())).unwrap();
        let mut vars: Vec<_> = bytes.split(|c| *c == 0).filter(|v| !v.is_empty()).collect();
        vars.sort();
        assert_eq!(
            vars,
            vec![
                b"LANG=C".as_slice(),
                b"LC_ALL=C".as_slice(),
                b"TZ=UTC".as_slice()
            ]
        );
    }
    run.cancel();
    run.wait();
    assert!(Sandbox::new(SandboxOptions::new("/missing/pi-codemode-worker")).is_err());
    #[cfg(unix)]
    {
        let sandbox = Sandbox::new(SandboxOptions::new("/usr/bin/false")).unwrap();
        let run = sandbox.start("", options()).unwrap();
        assert!(matches!(
            run.wait(),
            ExecutionResult::Error {
                error: ExecutionError {
                    kind: ErrorKind::Sandbox,
                    ..
                },
                ..
            }
        ));
        assert_eq!(sandbox.active_workers(), 0);
    }
}
#[test]
fn boundary_sizes_store_total_and_seed_fingerprint_are_observable() {
    let result = sandbox()
        .execute(
            "store('k','x'.repeat(256*1024-2));return load('k').length",
            ExecuteOptions::default(),
            |_, _| unreachable!(),
        )
        .unwrap();
    assert_eq!(value(result), json!(256 * 1024 - 2));
    let result = sandbox()
        .execute(
            "for(let i=0;i<5;i++)store('k'+i,'x'.repeat(240*1024))",
            ExecuteOptions::default(),
            |_, _| unreachable!(),
        )
        .unwrap();
    assert!(!result.is_ok());
    let mut observed = Vec::new();
    for seed in [0, 1, 2] {
        let mut o = options();
        o.seed = seed;
        let run = sandbox().start("return Math.random()", o).unwrap();
        let fingerprint = event(&run);
        observed.push((fingerprint, value(run.wait())));
    }
    assert_eq!(observed[0].0, observed[1].0);
    assert_eq!(observed[1].0, observed[2].0);
    assert_ne!(observed[0].1, observed[1].1);
    assert_ne!(observed[1].1, observed[2].1);
    let normal = sandbox().start("return 1", options()).unwrap();
    let first = event(&normal);
    normal.wait();
    let mut config = SandboxOptions::new(env!("CARGO_BIN_EXE_pi-codemode-worker"));
    config.memory_limit_bytes = 32 * 1024 * 1024;
    let limited = Sandbox::new(config)
        .unwrap()
        .start("return 1", options())
        .unwrap();
    assert_ne!(first, event(&limited));
    limited.wait();
}
#[test]
fn argument_and_call_count_bounds_are_terminal_and_json_dtos_use_booleans() {
    for code in [
        "try{tools.echo('x'.repeat(1024*1024))}catch{}return 1",
        "for(let i=0;i<4097;i++)tools.echo(i);return 1",
    ] {
        let mut o = options();
        o.tools.push(DeclaredTool::new("echo"));
        let result = sandbox().start(code, o).unwrap().wait();
        assert!(matches!(
            result,
            ExecutionResult::Error {
                error: ExecutionError {
                    kind: ErrorKind::Script,
                    ..
                },
                ..
            }
        ));
    }
    let result = sandbox()
        .execute("return 1", ExecuteOptions::default(), |_, _| unreachable!())
        .unwrap();
    assert_eq!(serde_json::to_value(&result).unwrap()["ok"], json!(true));
    assert_eq!(
        serde_json::to_value(Outcome::error("bad")).unwrap(),
        json!({"ok":false,"message":"bad"})
    );
    assert_eq!(
        serde_json::from_value::<ExecutionResult>(serde_json::to_value(&result).unwrap()).unwrap(),
        result
    );
}

#[test]
fn json_null_and_javascript_undefined_stay_distinct_at_every_bridge() {
    let mut o = options();
    o.tools.push(DeclaredTool::new("echo"));
    let run=sandbox().start("const a=await tools.echo(null);const b=await tools.echo();return [a===null,b===undefined]",o.clone()).unwrap();
    let a = call(&run);
    assert_eq!(a.args, Some(Value::Null));
    run.deliver(1, o.started_at, a.id, Outcome::success(Value::Null))
        .unwrap();
    let b = call(&run);
    assert_eq!(b.args, None);
    run.deliver(2, o.started_at, b.id, Outcome::undefined())
        .unwrap();
    assert_eq!(value(run.wait()), json!([true, true]));
    let result = sandbox()
        .execute(
            "return null",
            ExecuteOptions::default(),
            |_, _| unreachable!(),
        )
        .unwrap();
    assert!(matches!(
        &result,
        ExecutionResult::Success {
            value: Some(Value::Null),
            ..
        }
    ));
    assert_eq!(
        serde_json::from_value::<ExecutionResult>(serde_json::to_value(&result).unwrap()).unwrap(),
        result
    );
    let result = sandbox()
        .execute(
            "return undefined",
            ExecuteOptions::default(),
            |_, _| unreachable!(),
        )
        .unwrap();
    assert!(matches!(
        &result,
        ExecutionResult::Success { value: None, .. }
    ));
}

fn nested(levels: usize) -> Value {
    (0..levels).fold(json!(0), |value, _| Value::Array(vec![value]))
}
#[test]
fn native_json_supports_150_level_values_in_every_public_bridge() {
    let sandbox = sandbox();
    let mut o = options();
    o.tools.push(DeclaredTool::new("echo"));
    o.store.insert("initial".into(), nested(150));
    let run = sandbox.start("text('before');const a=load('initial');store('saved',a);const b=await tools.echo(a);return b", o.clone()).unwrap();
    let made = call(&run);
    assert_eq!(made.id, 1);
    assert_eq!(made.args, Some(nested(150)));
    run.deliver(1, o.started_at, made.id, Outcome::success(nested(150)))
        .unwrap();
    match run.wait() {
        ExecutionResult::Success {
            value,
            output,
            calls,
            store_writes,
            during,
        } => {
            assert_eq!(value, Some(nested(150)));
            assert_eq!(store_writes.set.get("saved"), Some(&nested(150)));
            assert_eq!(
                output,
                vec![OutputItem::Text {
                    text: "before".into(),
                    console: false
                }]
            );
            assert_eq!(calls[0].status, CallStatus::Ok);
            assert_eq!(during, Some(During { seq: 1, issued: 1 }));
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(sandbox.active_workers(), 0);
}
#[test]
fn rejected_native_json_calls_and_stores_are_catchable_without_skipping_ids() {
    for setup in [
        "let bad=0;for(let i=0;i<257;i++)bad=[bad]",
        r"const bad='\ud800'",
    ] {
        let sandbox = sandbox();
        let mut o = options();
        o.tools.push(DeclaredTool::new("echo"));
        let code = format!(
            "{setup};store('saved',1);const errors=[];try{{store('saved',bad)}}catch(e){{errors.push(e.message)}}try{{await tools.echo(bad)}}catch(e){{errors.push(e.message)}}const valid=await tools.echo(1);return [load('saved'),valid,errors]"
        );
        let run = sandbox.start(code, o.clone()).unwrap();
        let made = call(&run);
        assert_eq!(made.id, 1);
        assert_eq!(made.args, Some(json!(1)));
        assert_eq!(event(&run), Event::Idle { seq: 0, issued: 1 });
        run.deliver(1, o.started_at, made.id, Outcome::success(json!(2)))
            .unwrap();
        let result = run.wait();
        assert!(
            matches!(
                &result,
                ExecutionResult::Success {
                    during: Some(During { seq: 1, issued: 1 }),
                    ..
                }
            ),
            "{result:?}"
        );
        let answer = value(result);
        assert_eq!(answer[0], json!(1));
        assert_eq!(answer[1], json!(2));
        for error in answer[2].as_array().unwrap() {
            assert!(
                error.as_str().unwrap().contains("CodeMode JSON domain"),
                "{error:?}"
            );
        }
        assert_eq!(sandbox.active_workers(), 0);
    }
}
#[test]
fn unsupported_native_json_returns_and_store_keys_keep_terminal_attribution() {
    for code in [
        "text('before');let a=0;for(let i=0;i<257;i++)a=[a];return a",
        r"text('before');return '\ud800'",
        r"text('before');store('k','\ud800');return 1",
        r"text('before');store('\ud800',undefined);return 1",
    ] {
        let sandbox = sandbox();
        let run = sandbox.start(code, options()).unwrap();
        let pid = run.process_id();
        match run.wait() {
            ExecutionResult::Error {
                error,
                output,
                calls,
                during,
            } => {
                assert_eq!(error.kind, ErrorKind::Script);
                assert_eq!(error.name.as_deref(), Some("TypeError"));
                assert!(error.message.contains("CodeMode JSON domain"), "{error:?}");
                assert_eq!(
                    output,
                    vec![OutputItem::Text {
                        text: "before".into(),
                        console: false
                    }]
                );
                assert!(calls.is_empty());
                assert_eq!(during, Some(During { seq: 0, issued: 0 }));
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(sandbox.active_workers(), 0);
        #[cfg(target_os = "linux")]
        assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    }
}
#[test]
fn invalid_native_json_delivery_and_initial_store_fail_before_consumption() {
    let sandbox = sandbox();
    let mut initial = options();
    initial.store.insert("bad".into(), nested(257));
    assert!(sandbox.start("", initial).is_err());
    assert_eq!(sandbox.active_workers(), 0);
    let mut o = options();
    o.tools.push(DeclaredTool::new("echo"));
    let run = sandbox
        .start(
            "const result=await tools.echo();store('saved',result);return result",
            o.clone(),
        )
        .unwrap();
    let made = call(&run);
    assert!(
        run.deliver(1, o.started_at, made.id, Outcome::success(nested(257)))
            .unwrap_err()
            .0
            .contains("CodeMode JSON domain")
    );
    assert!(
        run.deliver(
            1,
            o.started_at,
            made.id,
            Outcome::Error {
                message: "bad".into(),
                properties: [("deep".into(), nested(256))].into()
            }
        )
        .is_err()
    );
    run.deliver(1, o.started_at, made.id, Outcome::success(nested(256)))
        .unwrap();
    match run.wait() {
        ExecutionResult::Success {
            value,
            store_writes,
            calls,
            during,
            ..
        } => {
            assert_eq!(value, Some(nested(256)));
            assert_eq!(store_writes.set.get("saved"), Some(&nested(256)));
            assert_eq!(calls[0].status, CallStatus::Ok);
            assert_eq!(during, Some(During { seq: 1, issued: 1 }));
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(sandbox.active_workers(), 0);
    let good = sandbox
        .execute(
            r"return '\ud83d\ude00'",
            ExecuteOptions::default(),
            |_, _| unreachable!(),
        )
        .unwrap();
    assert_eq!(value(good), json!("😀"));
}
#[test]
fn execute_rejects_oversized_callback_outcome_as_tool_error_and_waits_for_exit() {
    let sandbox = sandbox();
    let cancelled = Arc::new(AtomicBool::new(false));
    let observed = cancelled.clone();
    let result=sandbox.execute("text('before');tools.wait();try{await tools.echo(1)}catch(e){text(e.message);return 'caught'}",ExecuteOptions { tools:vec![DeclaredTool::new("wait"),DeclaredTool::new("echo")],..Default::default() },move|call,token|{
        if call.name=="echo" {return Outcome::success(json!("x".repeat(MAX_JSON_BYTES)))}
        let deadline=Instant::now()+Duration::from_secs(3);
        while !token.is_cancelled()&&Instant::now()<deadline {std::thread::sleep(Duration::from_millis(1));}
        observed.store(token.is_cancelled(),Ordering::Release);Outcome::undefined()
    }).expect("admitted execute must return a typed result");
    match result {
        ExecutionResult::Success {
            value,
            output,
            calls,
            ..
        } => {
            assert_eq!(value, Some(json!("caught")));
            assert_eq!(calls[0].status, CallStatus::Cancelled);
            assert_eq!(calls[1].status, CallStatus::Error);
            assert_eq!(
                output[0],
                OutputItem::Text {
                    text: "before".into(),
                    console: false
                }
            );
            assert!(
                matches!(&output[1],OutputItem::Text { text,.. }if text.contains("CodeMode result bridge limit exceeded"))
            );
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(sandbox.active_workers(), 0);
    let deadline = Instant::now() + Duration::from_secs(3);
    while !cancelled.load(Ordering::Acquire) && Instant::now() < deadline {
        std::thread::yield_now()
    }
    assert!(cancelled.load(Ordering::Acquire));
}
#[test]
fn execute_exhausted_delivery_budget_returns_typed_failure_with_prior_output() {
    let sandbox = sandbox();
    let result=sandbox.execute("text('before');store('unsaved',1);for(let i=0;i<18;i++){try{await tools.echo(i)}catch{}}return 1",ExecuteOptions { tools:vec![DeclaredTool::new("echo")],..Default::default() },|_,_|Outcome::success(json!("x".repeat(MAX_JSON_BYTES-32)))).expect("post-admission failure stays typed");
    match result {
        ExecutionResult::Error {
            error,
            output,
            calls,
            during,
        } => {
            assert_eq!(error.kind, ErrorKind::Sandbox);
            assert!(
                error
                    .message
                    .contains("CodeMode result bridge limit exceeded")
            );
            assert_eq!(
                output,
                vec![OutputItem::Text {
                    text: "before".into(),
                    console: false
                }]
            );
            assert_eq!(calls[0].status, CallStatus::Ok);
            assert_eq!(calls.last().unwrap().status, CallStatus::Cancelled);
            assert_eq!(during, None);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(sandbox.active_workers(), 0);
}

#[test]
fn native_json_terminal_failure_after_delivery_retains_calls_and_during() {
    let sandbox = sandbox();
    let mut o = options();
    o.tools.push(DeclaredTool::new("echo"));
    let run = sandbox
        .start(
            r"await tools.echo(1);text('before');return '\ud800'",
            o.clone(),
        )
        .unwrap();
    let made = call(&run);
    run.deliver(1, o.started_at, made.id, Outcome::success(json!(1)))
        .unwrap();
    match run.wait() {
        ExecutionResult::Error {
            error,
            output,
            calls,
            during,
        } => {
            assert_eq!(error.kind, ErrorKind::Script);
            assert!(error.message.contains("CodeMode JSON domain"));
            assert_eq!(
                output,
                vec![OutputItem::Text {
                    text: "before".into(),
                    console: false
                }]
            );
            assert_eq!(calls[0].status, CallStatus::Ok);
            assert_eq!(during, Some(During { seq: 1, issued: 1 }));
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(sandbox.active_workers(), 0);
}

#[test]
fn closing_waiting_worker_reports_aborted_without_terminal_during() {
    // Closing stdin can make the worker report an EOF-derived error while the
    // monitor is receiving. The admitted host cancellation must win that wait.
    for _ in 0..32 {
        let sandbox = sandbox();
        let mut options = options();
        options.tools.push(DeclaredTool::new("hold"));
        let run = sandbox.start("await tools.hold({})", options).unwrap();
        assert!(matches!(event(&run), Event::Started { .. }));
        assert_eq!(call(&run).id, 1);
        assert_eq!(event(&run), Event::Idle { seq: 0, issued: 1 });
        sandbox.close();
        let result = run.wait();
        assert!(
            matches!(
                &result,
                ExecutionResult::Error {
                    error: ExecutionError {
                        kind: ErrorKind::Aborted,
                        ..
                    },
                    during: None,
                    ..
                }
            ),
            "{result:?}"
        );
        assert_eq!(sandbox.active_workers(), 0);
    }
}
