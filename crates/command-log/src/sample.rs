//! A log that holds every record and hole variant and the byte form's hard
//! cases: the golden log of F2 (ADR-0046), and the round trip of F1.

use crate::log::FORMAT_VERSION;
use crate::record::*;

fn subject(kind: &str, scope: &[u8], index: u64) -> Subject {
    Subject {
        kind: kind.to_owned(),
        scope: scope.to_vec(),
        index,
    }
}

fn step(subject: Subject, label: &str, n: u32) -> StepId {
    StepId {
        subject,
        label: label.to_owned(),
        n,
    }
}

fn field(name: &str, value: Value) -> Field {
    Field {
        name: name.to_owned(),
        value,
    }
}

/// The golden log's header.
pub fn header() -> Header {
    Header {
        format: FORMAT_VERSION,
        algorithm: "ef4077eedd17cf26cf1fb4158ea9f242".to_owned(),
        scenario: "golden — every variant".to_owned(),
        revision: "c1".to_owned(),
        profile: Profile {
            name: "policy".to_owned(),
            rendered: vec!["device".to_owned()],
            captured: vec!["group".to_owned(), "policy".to_owned()],
            gapless: vec!["lifecycle".to_owned()],
        },
        window: 600_000_000,
        read_ahead: 60_000_000,
        retained: vec!["create".to_owned()],
        laws: vec![Law {
            name: "device.os".to_owned(),
            law: "categorical[2476979795053773,4503599627370496]".to_owned(),
        }],
        operations: vec![Operation {
            name: "createDeviceGroup".to_owned(),
            method: "POST".to_owned(),
            path: "/v1/device-groups".to_owned(),
            content_type: "application/json".to_owned(),
            decorated: vec!["Authorization".to_owned()],
            writes: vec!["etag".to_owned()],
            retry: Retry::Never,
        }],
    }
}

/// The golden log's steps, in their order.
pub fn records() -> Vec<Record> {
    // Scope bytes whose base64 has `+`, `/` and `=`.
    let tenant: &[u8] = &[0xfb, 0xff, 0xbf, 0x02];
    let device = subject("device", tenant, u64::MAX);
    let group = subject("group", tenant, 17);
    let created = step(group.clone(), "create", 0);
    vec![
        Record {
            step: step(subject("tenant", &[], 0), "onboard", 0),
            at: 0,
            release: Release::At,
            after: vec![],
            action: Action::Host(HostCall {
                op: "onboard".to_owned(),
                args: Payload {
                    codec: Codec::Text,
                    bytes: "tenant «Ω» \"q\" \\ \n".as_bytes().to_vec(),
                    holes: vec![],
                },
            }),
            expect: vec![Outcome::Observed],
            capture: vec![],
            measure: vec![],
            deviation: None,
        },
        Record {
            step: created.clone(),
            at: 1_000,
            release: Release::Ready { think: 50_000 },
            after: vec![Dep::Step {
                step: step(subject("tenant", &[], 0), "onboard", 0),
                on: On::Expected,
            }],
            action: Action::Rest(Rest {
                op: "createDeviceGroup".to_owned(),
                caller: Some(subject("principal", tenant, 0)),
                path: vec![],
                query: vec![field("dry", Value::Literal("false".to_owned()))],
                headers: vec![field(
                    "X-Request-Id",
                    Value::Hole(Hole::Rendered {
                        of: Of::Step(created.clone()),
                        role: "request".to_owned(),
                        rendering: Rendering::Hex { len: 8 },
                    }),
                )],
                body: Some(Payload {
                    codec: Codec::Json,
                    bytes: br#"{"name":"branch-017","members":["00000000-0000-4000-8000-000000000000"]}"#.to_vec(),
                    holes: vec![Span {
                        offset: 32,
                        len: 38,
                        field: "/members/0".to_owned(),
                        hole: Hole::Rendered {
                            of: Of::Subject(device.clone()),
                            role: "id".to_owned(),
                            rendering: Rendering::UuidText,
                        },
                        form: Form::JsonToken,
                    }],
                }),
            }),
            expect: vec![Outcome::Response {
                status: 201,
                problem: None,
                body: vec![Predicate {
                    pointer: "/name".to_owned(),
                    test: Test::Equals(Value::Literal("\"branch-017\"".to_owned())),
                }],
            }],
            capture: vec![
                Capture {
                    subject: group.clone(),
                    slot: "id".to_owned(),
                    source: Source::Json("/device_group_id".to_owned()),
                    secret: false,
                },
                Capture {
                    subject: group.clone(),
                    slot: "etag".to_owned(),
                    source: Source::Header("ETag".to_owned()),
                    secret: false,
                },
                Capture {
                    subject: group.clone(),
                    slot: "location".to_owned(),
                    source: Source::Location,
                    secret: true,
                },
            ],
            measure: vec![],
            deviation: None,
        },
        Record {
            step: step(device.clone(), "managed", 0),
            at: 2_000,
            release: Release::At,
            after: vec![
                Dep::Step {
                    step: created.clone(),
                    on: On::Sent,
                },
                Dep::Each {
                    kind: "group".to_owned(),
                    scope: tenant.to_vec(),
                    label: "create".to_owned(),
                    n: 0,
                    on: On::Settled,
                    count: Some(100),
                },
            ],
            action: Action::Bus(Bus {
                channel: "device.lifecycle".to_owned(),
                key: Value::Hole(Hole::Rendered {
                    of: Of::Subject(device.clone()),
                    role: "id".to_owned(),
                    rendering: Rendering::UuidText,
                }),
                headers: vec![field(
                    "message_id",
                    Value::Hole(Hole::Rendered {
                        of: Of::Step(step(device.clone(), "managed", 0)),
                        role: "message".to_owned(),
                        rendering: Rendering::UuidText,
                    }),
                )],
                stream: Some(StreamPosition {
                    name: "lifecycle".to_owned(),
                    position: 1,
                }),
                payload: Payload {
                    codec: Codec::Protobuf,
                    bytes: vec![0x0a, 0x10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x12, 0x02, 0xfb, 0xff],
                    holes: vec![Span {
                        offset: 2,
                        len: 16,
                        field: "1".to_owned(),
                        hole: Hole::Rendered {
                            of: Of::Subject(device.clone()),
                            role: "id".to_owned(),
                            rendering: Rendering::UuidBytes,
                        },
                        form: Form::Raw,
                    }],
                },
            }),
            expect: vec![Outcome::Delivered],
            capture: vec![],
            measure: vec![],
            deviation: Some(Deviation {
                rule: "stream.predecessor".to_owned(),
                reason: "the first message of the stream".to_owned(),
            }),
        },
        Record {
            step: step(device.clone(), "bundle", 0),
            at: 3_000,
            release: Release::Ready { think: 0 },
            after: vec![Dep::Step {
                step: step(device.clone(), "managed", 0),
                on: On::Expected,
            }],
            action: Action::Observe(Observe {
                probe: Probe::Rest(Rest {
                    op: "getDeviceEffectiveBundle".to_owned(),
                    caller: None,
                    path: vec![Value::Hole(Hole::Rendered {
                        of: Of::Subject(device.clone()),
                        role: "id".to_owned(),
                        rendering: Rendering::UuidText,
                    })],
                    query: vec![],
                    headers: vec![field(
                        "If-None-Match",
                        Value::Hole(Hole::Captured {
                            subject: group.clone(),
                            slot: "etag".to_owned(),
                            from: created.clone(),
                        }),
                    )],
                    body: None,
                }),
                until: vec![
                    Predicate {
                        pointer: "/policies".to_owned(),
                        test: Test::Contains(Value::Hole(Hole::Captured {
                            subject: group.clone(),
                            slot: "id".to_owned(),
                            from: created.clone(),
                        })),
                    },
                    Predicate {
                        pointer: "/version".to_owned(),
                        test: Test::Exists,
                    },
                ],
                every: 5_000_000,
                timeout: 300_000_000,
            }),
            expect: vec![
                Outcome::Observed,
                Outcome::Response {
                    status: 409,
                    problem: Some("policy.draft_unchanged".to_owned()),
                    body: vec![],
                },
            ],
            capture: vec![],
            measure: vec![Measure {
                metric: "placement".to_owned(),
                from: step(device.clone(), "managed", 0),
            }],
            deviation: None,
        },
        Record {
            step: step(device.clone(), "report", 0),
            at: u64::MAX,
            release: Release::At,
            after: vec![Dep::Each {
                kind: "device".to_owned(),
                scope: vec![],
                label: "managed".to_owned(),
                n: 1,
                on: On::Expected,
                count: None,
            }],
            action: Action::Observe(Observe {
                probe: Probe::Bus {
                    channel: "device.facts".to_owned(),
                    key: Value::Literal("ключ".to_owned()),
                    event: "device.facts_reported".to_owned(),
                },
                until: vec![],
                every: 0,
                timeout: 1,
            }),
            expect: vec![Outcome::Observed],
            capture: vec![],
            measure: vec![],
            deviation: None,
        },
        Record {
            step: step(device, "stamped", u32::MAX),
            at: u64::MAX,
            release: Release::At,
            after: vec![],
            action: Action::Host(HostCall {
                op: "stamp".to_owned(),
                args: Payload {
                    codec: Codec::Json,
                    bytes: br#"{"at":"0","ms":"0"}"#.to_vec(),
                    holes: vec![
                        Span {
                            offset: 6,
                            len: 3,
                            field: "/at".to_owned(),
                            hole: Hole::Time {
                                base: TimeBase::RunStart,
                                offset: i64::MIN,
                                rendering: TimeRendering::Rfc3339Micros,
                            },
                            form: Form::JsonToken,
                        },
                        Span {
                            offset: 15,
                            len: 3,
                            field: "/ms".to_owned(),
                            hole: Hole::Time {
                                base: TimeBase::Sent,
                                offset: -1,
                                rendering: TimeRendering::EpochMillis,
                            },
                            form: Form::JsonToken,
                        },
                    ],
                },
            }),
            expect: vec![Outcome::Response {
                status: 200,
                problem: None,
                body: vec![],
            }],
            capture: vec![Capture {
                subject: subject("tenant", &[], 0),
                slot: "secret".to_owned(),
                source: Source::Json("/secret".to_owned()),
                secret: true,
            }],
            measure: vec![],
            deviation: None,
        },
        Record {
            step: step(subject("key", &[1], 1), "keys", 0),
            at: u64::MAX,
            release: Release::At,
            after: vec![],
            action: Action::Host(HostCall {
                op: "keys".to_owned(),
                args: Payload {
                    codec: Codec::Json,
                    bytes: br#"{"k":"x"}"#.to_vec(),
                    holes: vec![Span {
                        offset: 5,
                        len: 3,
                        field: "/k".to_owned(),
                        hole: Hole::Rendered {
                            of: Of::Subject(subject("key", &[1], 1)),
                            role: "dpop".to_owned(),
                            rendering: Rendering::Secret32,
                        },
                        form: Form::JsonToken,
                    }],
                },
            }),
            expect: vec![Outcome::Observed],
            capture: vec![],
            measure: vec![],
            deviation: None,
        },
    ]
}
