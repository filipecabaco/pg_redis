use pgrx::datum::DatumWithOid;
use pgrx::spi::SpiClient;

#[derive(Debug)]
pub enum Command {
    Ping {
        msg: Option<Vec<u8>>,
    },
    Echo {
        msg: Vec<u8>,
    },
    Select {
        db: u8,
    },
    Auth {
        password: String,
    },
    Info,
    Hello {
        proto: Option<u8>,
        auth: Option<String>,
        setname: Option<String>,
    },
    Reset,
    Quit,
    // CLIENT subcommands
    ClientId,
    ClientGetname,
    ClientSetname { name: String },
    ClientSetinfo,
    ClientList,
    ClientInfo,
    ClientNoEvict,
    ClientNoTouch,
    ClientOther,
    // COMMAND subcommands
    CommandCount,
    CommandInfo,
    CommandDocs,
    CommandList,
    CommandOther,
    // CONFIG subcommands
    ConfigGet { pattern: String },
    ConfigSet,
    ConfigOther,
    // Key-value commands
    Get {
        key: String,
    },
    Set {
        key: String,
        value: String,
        ex_ms: Option<i64>,
    },
    SetEx {
        key: String,
        value: String,
        ex_secs: i64,
    },
    PSetEx {
        key: String,
        value: String,
        ex_ms: i64,
    },
    MGet {
        keys: Vec<String>,
    },
    MSet {
        pairs: Vec<(String, String)>,
    },
    Del {
        keys: Vec<String>,
    },
    Exists {
        keys: Vec<String>,
    },
    Expire {
        key: String,
        secs: i64,
    },
    PExpire {
        key: String,
        ms: i64,
    },
    ExpireAt {
        key: String,
        unix_secs: i64,
    },
    PExpireAt {
        key: String,
        unix_ms: i64,
    },
    Ttl {
        key: String,
    },
    PTtl {
        key: String,
    },
    Persist {
        key: String,
    },
    ExpireTime {
        key: String,
    },
    PExpireTime {
        key: String,
    },
    // String commands
    Incr {
        key: String,
    },
    Decr {
        key: String,
    },
    IncrBy {
        key: String,
        delta: i64,
    },
    DecrBy {
        key: String,
        delta: i64,
    },
    IncrByFloat {
        key: String,
        delta: f64,
    },
    Append {
        key: String,
        value: String,
    },
    Strlen {
        key: String,
    },
    GetDel {
        key: String,
    },
    GetSet {
        key: String,
        value: String,
    },
    SetNx {
        key: String,
        value: String,
    },
    MSetNx {
        pairs: Vec<(String, String)>,
    },
    // Key inspection commands
    Type {
        key: String,
    },
    Keys {
        pattern: String,
    },
    DbSize,
    Unlink {
        keys: Vec<String>,
    },
    Rename {
        key: String,
        newkey: String,
    },
    RandomKey,
    Scan {
        cursor: u64,
        pattern: Option<String>,
        count: Option<i64>,
    },
    // Hash commands
    HGet {
        key: String,
        field: String,
    },
    HSet {
        key: String,
        pairs: Vec<(String, String)>,
    },
    HDel {
        key: String,
        fields: Vec<String>,
    },
    HGetAll {
        key: String,
    },
    HMGet {
        key: String,
        fields: Vec<String>,
    },
    HMSet {
        key: String,
        pairs: Vec<(String, String)>,
    },
    HKeys {
        key: String,
    },
    HVals {
        key: String,
    },
    HExists {
        key: String,
        field: String,
    },
    HLen {
        key: String,
    },
    HIncrBy {
        key: String,
        field: String,
        delta: i64,
    },
    HSetNx {
        key: String,
        field: String,
        value: String,
    },
}

pub enum Response {
    Pong(Option<Vec<u8>>),
    Null,
    Ok,
    Integer(i64),
    BulkString(Vec<u8>),
    SimpleString(String),
    Array(Vec<Option<Vec<u8>>>),
    ScanResult { keys: Vec<Option<Vec<u8>>> },
    Error(String),
}

impl Command {
    pub fn parse(parts: Vec<Vec<u8>>) -> Result<Command, String> {
        if parts.is_empty() {
            return Err("empty command".to_string());
        }
        let cmd = String::from_utf8_lossy(&parts[0]).to_uppercase();
        let args = &parts[1..];

        match cmd.as_str() {
            "PING" => Ok(Command::Ping {
                msg: args.first().cloned(),
            }),
            "ECHO" => {
                let msg = args.first().cloned().ok_or("ECHO requires argument")?;
                Ok(Command::Echo { msg })
            }
            "SELECT" => {
                let db: u8 = str_arg(args, 0, "SELECT")?
                    .parse()
                    .map_err(|_| "SELECT requires integer db".to_string())?;
                Ok(Command::Select { db })
            }
            "AUTH" => Ok(Command::Auth {
                // AUTH <password> or AUTH <username> <password> — password is always last.
                password: str_arg(args, args.len().saturating_sub(1), "AUTH")
                    .unwrap_or_default(),
            }),
            "INFO" => Ok(Command::Info),
            "HELLO" => {
                let proto = if args.is_empty() {
                    None
                } else {
                    let v: u8 = str_arg(args, 0, "HELLO")?
                        .parse()
                        .map_err(|_| "Protocol version is not an integer or out of range".to_string())?;
                    if v != 2 && v != 3 {
                        return Err("NOPROTO unsupported protocol version".to_string());
                    }
                    Some(v)
                };
                let mut auth: Option<String> = None;
                let mut setname: Option<String> = None;
                let mut i = 1;
                while i < args.len() {
                    let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                    match opt.as_str() {
                        "AUTH" => {
                            let password = str_arg(args, i + 2, "HELLO AUTH")
                                .or_else(|_| str_arg(args, i + 1, "HELLO AUTH"))?;
                            auth = Some(password);
                            i += if i + 2 < args.len() { 3 } else { 2 };
                        }
                        "SETNAME" => {
                            setname = Some(str_arg(args, i + 1, "HELLO SETNAME")?);
                            i += 2;
                        }
                        _ => { i += 1; }
                    }
                }
                Ok(Command::Hello { proto, auth, setname })
            }
            "RESET" => Ok(Command::Reset),
            "QUIT" => Ok(Command::Quit),
            "COMMAND" => {
                let sub = args.first().map(|a| String::from_utf8_lossy(a).to_uppercase());
                match sub.as_deref() {
                    Some("COUNT") => Ok(Command::CommandCount),
                    Some("INFO") => Ok(Command::CommandInfo),
                    Some("DOCS") => Ok(Command::CommandDocs),
                    Some("LIST") => Ok(Command::CommandList),
                    _ => Ok(Command::CommandOther),
                }
            }
            "CLIENT" => {
                let sub = args.first().map(|a| String::from_utf8_lossy(a).to_uppercase());
                match sub.as_deref() {
                    Some("ID") => Ok(Command::ClientId),
                    Some("GETNAME") => Ok(Command::ClientGetname),
                    Some("SETNAME") => Ok(Command::ClientSetname {
                        name: str_arg(args, 1, "CLIENT SETNAME")?,
                    }),
                    Some("SETINFO") => Ok(Command::ClientSetinfo),
                    Some("LIST") => Ok(Command::ClientList),
                    Some("INFO") => Ok(Command::ClientInfo),
                    Some("NO-EVICT") => Ok(Command::ClientNoEvict),
                    Some("NO-TOUCH") => Ok(Command::ClientNoTouch),
                    _ => Ok(Command::ClientOther),
                }
            }
            "CONFIG" => {
                let sub = args.first().map(|a| String::from_utf8_lossy(a).to_uppercase());
                match sub.as_deref() {
                    Some("GET") => Ok(Command::ConfigGet {
                        pattern: str_arg(args, 1, "CONFIG GET")?,
                    }),
                    Some("SET") => Ok(Command::ConfigSet),
                    _ => Ok(Command::ConfigOther),
                }
            }
            "GET" => Ok(Command::Get {
                key: str_arg(args, 0, "GET")?,
            }),
            "SET" => {
                let key = str_arg(args, 0, "SET")?;
                let value = str_arg(args, 1, "SET")?;
                let mut ex_ms: Option<i64> = None;
                let mut i = 2;
                while i < args.len() {
                    let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                    match opt.as_str() {
                        "EX" => {
                            let secs: i64 = str_arg(args, i + 1, "SET EX")?
                                .parse()
                                .map_err(|_| "EX requires integer".to_string())?;
                            ex_ms = Some(secs * 1000);
                            i += 2;
                        }
                        "PX" => {
                            let ms: i64 = str_arg(args, i + 1, "SET PX")?
                                .parse()
                                .map_err(|_| "PX requires integer".to_string())?;
                            ex_ms = Some(ms);
                            i += 2;
                        }
                        _ => {
                            i += 1;
                        }
                    }
                }
                Ok(Command::Set { key, value, ex_ms })
            }
            "SETEX" => Ok(Command::SetEx {
                key: str_arg(args, 0, "SETEX")?,
                ex_secs: str_arg(args, 1, "SETEX")?
                    .parse()
                    .map_err(|_| "SETEX requires integer seconds".to_string())?,
                value: str_arg(args, 2, "SETEX")?,
            }),
            "PSETEX" => Ok(Command::PSetEx {
                key: str_arg(args, 0, "PSETEX")?,
                ex_ms: str_arg(args, 1, "PSETEX")?
                    .parse()
                    .map_err(|_| "PSETEX requires integer ms".to_string())?,
                value: str_arg(args, 2, "PSETEX")?,
            }),
            "MGET" => {
                if args.is_empty() {
                    return Err("MGET requires at least one key".to_string());
                }
                Ok(Command::MGet {
                    keys: args
                        .iter()
                        .map(|a| String::from_utf8_lossy(a).into_owned())
                        .collect(),
                })
            }
            "MSET" => {
                if args.len() < 2 || !args.len().is_multiple_of(2) {
                    return Err("MSET requires pairs of key value".to_string());
                }
                let pairs = args
                    .chunks(2)
                    .map(|c| {
                        (
                            String::from_utf8_lossy(&c[0]).into_owned(),
                            String::from_utf8_lossy(&c[1]).into_owned(),
                        )
                    })
                    .collect();
                Ok(Command::MSet { pairs })
            }
            "DEL" => {
                if args.is_empty() {
                    return Err("DEL requires at least one key".to_string());
                }
                Ok(Command::Del {
                    keys: args
                        .iter()
                        .map(|a| String::from_utf8_lossy(a).into_owned())
                        .collect(),
                })
            }
            "EXISTS" => {
                if args.is_empty() {
                    return Err("EXISTS requires at least one key".to_string());
                }
                Ok(Command::Exists {
                    keys: args
                        .iter()
                        .map(|a| String::from_utf8_lossy(a).into_owned())
                        .collect(),
                })
            }
            "EXPIRE" => Ok(Command::Expire {
                key: str_arg(args, 0, "EXPIRE")?,
                secs: str_arg(args, 1, "EXPIRE")?
                    .parse()
                    .map_err(|_| "EXPIRE requires integer".to_string())?,
            }),
            "PEXPIRE" => Ok(Command::PExpire {
                key: str_arg(args, 0, "PEXPIRE")?,
                ms: str_arg(args, 1, "PEXPIRE")?
                    .parse()
                    .map_err(|_| "PEXPIRE requires integer".to_string())?,
            }),
            "EXPIREAT" => Ok(Command::ExpireAt {
                key: str_arg(args, 0, "EXPIREAT")?,
                unix_secs: str_arg(args, 1, "EXPIREAT")?
                    .parse()
                    .map_err(|_| "EXPIREAT requires integer".to_string())?,
            }),
            "PEXPIREAT" => Ok(Command::PExpireAt {
                key: str_arg(args, 0, "PEXPIREAT")?,
                unix_ms: str_arg(args, 1, "PEXPIREAT")?
                    .parse()
                    .map_err(|_| "PEXPIREAT requires integer".to_string())?,
            }),
            "TTL" => Ok(Command::Ttl {
                key: str_arg(args, 0, "TTL")?,
            }),
            "PTTL" => Ok(Command::PTtl {
                key: str_arg(args, 0, "PTTL")?,
            }),
            "PERSIST" => Ok(Command::Persist {
                key: str_arg(args, 0, "PERSIST")?,
            }),
            "EXPIRETIME" => Ok(Command::ExpireTime {
                key: str_arg(args, 0, "EXPIRETIME")?,
            }),
            "PEXPIRETIME" => Ok(Command::PExpireTime {
                key: str_arg(args, 0, "PEXPIRETIME")?,
            }),
            "INCR" => Ok(Command::Incr {
                key: str_arg(args, 0, "INCR")?,
            }),
            "DECR" => Ok(Command::Decr {
                key: str_arg(args, 0, "DECR")?,
            }),
            "INCRBY" => Ok(Command::IncrBy {
                key: str_arg(args, 0, "INCRBY")?,
                delta: str_arg(args, 1, "INCRBY")?
                    .parse()
                    .map_err(|_| "INCRBY requires integer".to_string())?,
            }),
            "DECRBY" => Ok(Command::DecrBy {
                key: str_arg(args, 0, "DECRBY")?,
                delta: str_arg(args, 1, "DECRBY")?
                    .parse()
                    .map_err(|_| "DECRBY requires integer".to_string())?,
            }),
            "INCRBYFLOAT" => Ok(Command::IncrByFloat {
                key: str_arg(args, 0, "INCRBYFLOAT")?,
                delta: str_arg(args, 1, "INCRBYFLOAT")?
                    .parse()
                    .map_err(|_| "INCRBYFLOAT requires float".to_string())?,
            }),
            "APPEND" => Ok(Command::Append {
                key: str_arg(args, 0, "APPEND")?,
                value: str_arg(args, 1, "APPEND")?,
            }),
            "STRLEN" => Ok(Command::Strlen {
                key: str_arg(args, 0, "STRLEN")?,
            }),
            "GETDEL" => Ok(Command::GetDel {
                key: str_arg(args, 0, "GETDEL")?,
            }),
            "GETSET" => Ok(Command::GetSet {
                key: str_arg(args, 0, "GETSET")?,
                value: str_arg(args, 1, "GETSET")?,
            }),
            "SETNX" => Ok(Command::SetNx {
                key: str_arg(args, 0, "SETNX")?,
                value: str_arg(args, 1, "SETNX")?,
            }),
            "MSETNX" => {
                if args.len() < 2 || !args.len().is_multiple_of(2) {
                    return Err("MSETNX requires pairs of key value".to_string());
                }
                let pairs = args
                    .chunks(2)
                    .map(|c| {
                        (
                            String::from_utf8_lossy(&c[0]).into_owned(),
                            String::from_utf8_lossy(&c[1]).into_owned(),
                        )
                    })
                    .collect();
                Ok(Command::MSetNx { pairs })
            }
            "TYPE" => Ok(Command::Type {
                key: str_arg(args, 0, "TYPE")?,
            }),
            "KEYS" => Ok(Command::Keys {
                pattern: str_arg(args, 0, "KEYS")?,
            }),
            "DBSIZE" => Ok(Command::DbSize),
            "UNLINK" => {
                if args.is_empty() {
                    return Err("UNLINK requires at least one key".to_string());
                }
                Ok(Command::Unlink {
                    keys: args
                        .iter()
                        .map(|a| String::from_utf8_lossy(a).into_owned())
                        .collect(),
                })
            }
            "RENAME" => Ok(Command::Rename {
                key: str_arg(args, 0, "RENAME")?,
                newkey: str_arg(args, 1, "RENAME")?,
            }),
            "RANDOMKEY" => Ok(Command::RandomKey),
            "SCAN" => {
                let cursor: u64 = str_arg(args, 0, "SCAN")?
                    .parse()
                    .map_err(|_| "SCAN requires integer cursor".to_string())?;
                let mut pattern: Option<String> = None;
                let mut count: Option<i64> = None;
                let mut i = 1;
                while i < args.len() {
                    let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                    match opt.as_str() {
                        "MATCH" => {
                            pattern = Some(str_arg(args, i + 1, "SCAN MATCH")?);
                            i += 2;
                        }
                        "COUNT" => {
                            count = Some(
                                str_arg(args, i + 1, "SCAN COUNT")?
                                    .parse()
                                    .map_err(|_| "SCAN COUNT requires integer".to_string())?,
                            );
                            i += 2;
                        }
                        _ => {
                            i += 1;
                        }
                    }
                }
                Ok(Command::Scan { cursor, pattern, count })
            }
            "HGET" => Ok(Command::HGet {
                key: str_arg(args, 0, "HGET")?,
                field: str_arg(args, 1, "HGET")?,
            }),
            "HSET" => {
                if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
                    return Err("HSET requires key field value [field value ...]".to_string());
                }
                let key = str_arg(args, 0, "HSET")?;
                let pairs = args[1..]
                    .chunks(2)
                    .map(|c| {
                        (
                            String::from_utf8_lossy(&c[0]).into_owned(),
                            String::from_utf8_lossy(&c[1]).into_owned(),
                        )
                    })
                    .collect();
                Ok(Command::HSet { key, pairs })
            }
            "HDEL" => {
                if args.len() < 2 {
                    return Err("HDEL requires key and at least one field".to_string());
                }
                Ok(Command::HDel {
                    key: str_arg(args, 0, "HDEL")?,
                    fields: args[1..]
                        .iter()
                        .map(|a| String::from_utf8_lossy(a).into_owned())
                        .collect(),
                })
            }
            "HGETALL" => Ok(Command::HGetAll {
                key: str_arg(args, 0, "HGETALL")?,
            }),
            "HMGET" => {
                if args.len() < 2 {
                    return Err("HMGET requires key and at least one field".to_string());
                }
                Ok(Command::HMGet {
                    key: str_arg(args, 0, "HMGET")?,
                    fields: args[1..]
                        .iter()
                        .map(|a| String::from_utf8_lossy(a).into_owned())
                        .collect(),
                })
            }
            "HMSET" => {
                if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
                    return Err("HMSET requires key field value [field value ...]".to_string());
                }
                let key = str_arg(args, 0, "HMSET")?;
                let pairs = args[1..]
                    .chunks(2)
                    .map(|c| {
                        (
                            String::from_utf8_lossy(&c[0]).into_owned(),
                            String::from_utf8_lossy(&c[1]).into_owned(),
                        )
                    })
                    .collect();
                Ok(Command::HMSet { key, pairs })
            }
            "HKEYS" => Ok(Command::HKeys {
                key: str_arg(args, 0, "HKEYS")?,
            }),
            "HVALS" => Ok(Command::HVals {
                key: str_arg(args, 0, "HVALS")?,
            }),
            "HEXISTS" => Ok(Command::HExists {
                key: str_arg(args, 0, "HEXISTS")?,
                field: str_arg(args, 1, "HEXISTS")?,
            }),
            "HLEN" => Ok(Command::HLen {
                key: str_arg(args, 0, "HLEN")?,
            }),
            "HINCRBY" => Ok(Command::HIncrBy {
                key: str_arg(args, 0, "HINCRBY")?,
                field: str_arg(args, 1, "HINCRBY")?,
                delta: str_arg(args, 2, "HINCRBY")?
                    .parse()
                    .map_err(|_| "HINCRBY requires integer".to_string())?,
            }),
            "HSETNX" => Ok(Command::HSetNx {
                key: str_arg(args, 0, "HSETNX")?,
                field: str_arg(args, 1, "HSETNX")?,
                value: str_arg(args, 2, "HSETNX")?,
            }),
            _ => Err(format!("unknown command '{}'", cmd)),
        }
    }

    pub fn execute(&self, client: &mut SpiClient<'_>, db: u8) -> Response {
        match self {
            Command::Ping { msg } => Response::Pong(msg.clone()),
            Command::Echo { msg } => Response::BulkString(msg.clone()),
            Command::Select { .. } => Response::Ok,
            Command::Auth { .. } => Response::Ok,
            Command::Info => Response::BulkString(
                format!(
                    "# Server\r\nredis_version:7.0.0\r\nmode:standalone\r\nos:PostgreSQL\r\ntable_mode:{}\r\n",
                    if db % 2 == 0 { "unlogged" } else { "logged" }
                )
                .into_bytes(),
            ),
            Command::Hello { .. } => Response::Ok,
            Command::Reset => Response::Ok,
            Command::Quit => Response::Ok,

            // CLIENT commands
            Command::ClientId => Response::Integer(0),
            Command::ClientGetname => Response::Null,
            Command::ClientSetname { .. } => Response::Ok,
            Command::ClientSetinfo => Response::Ok,
            Command::ClientList => Response::BulkString(
                b"id=0 addr=127.0.0.1:0 name= db=0 cmd=client\n".to_vec(),
            ),
            Command::ClientInfo => Response::BulkString(
                b"id=0 addr=127.0.0.1:0 name= db=0 cmd=client\n".to_vec(),
            ),
            Command::ClientNoEvict => Response::Ok,
            Command::ClientNoTouch => Response::Ok,
            Command::ClientOther => Response::Ok,

            // COMMAND commands
            Command::CommandCount => Response::Integer(100),
            Command::CommandInfo => Response::Array(vec![]),
            Command::CommandDocs => Response::Array(vec![]),
            Command::CommandList => Response::Array(vec![]),
            Command::CommandOther => Response::Array(vec![]),

            // CONFIG commands
            Command::ConfigGet { .. } => Response::Array(vec![]),
            Command::ConfigSet => Response::Ok,
            Command::ConfigOther => Response::Ok,

            Command::Get { key } => {
                // Lazy expiry: delete expired row first, then read.
                client
                    .update(
                        &format!(
                            "DELETE FROM redis.kv_{db} WHERE key = $1 AND expires_at <= now()"
                        ),
                        None,
                        &[key.as_str().into()],
                    )
                    .ok();
                let sql = format!(
                    "SELECT value FROM redis.kv_{db} WHERE key = $1 \
                     AND (expires_at IS NULL OR expires_at > now())"
                );
                match client.select(&sql, None, &[key.as_str().into()]) {
                    Ok(tbl) => match tbl.first().get::<String>(1) {
                        Ok(Some(v)) => Response::BulkString(v.into_bytes()),
                        _ => Response::Null,
                    },
                    Err(e) => {
                        eprintln!("pg_redis: GET error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::Set { key, value, ex_ms } => {
                let sql = format!(
                    "INSERT INTO redis.kv_{db} (key, value, expires_at) \
                     VALUES ($1, $2, CASE WHEN $3::bigint IS NULL THEN NULL \
                             ELSE now() + ($3::bigint * interval '1 millisecond') END) \
                     ON CONFLICT (key) DO UPDATE \
                     SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at"
                );
                let args: &[DatumWithOid] = &[
                    key.as_str().into(),
                    value.as_str().into(),
                    (*ex_ms).into(),
                ];
                match client.update(&sql, None, args) {
                    Ok(_) => Response::Ok,
                    Err(e) => {
                        eprintln!("pg_redis: SET error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::SetEx { key, value, ex_secs } => {
                let sql = format!(
                    "INSERT INTO redis.kv_{db} (key, value, expires_at) \
                     VALUES ($1, $2, now() + ($3::bigint * interval '1 second')) \
                     ON CONFLICT (key) DO UPDATE \
                     SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at"
                );
                let args: &[DatumWithOid] =
                    &[key.as_str().into(), value.as_str().into(), (*ex_secs).into()];
                match client.update(&sql, None, args) {
                    Ok(_) => Response::Ok,
                    Err(e) => {
                        eprintln!("pg_redis: SETEX error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::PSetEx { key, value, ex_ms } => {
                let sql = format!(
                    "INSERT INTO redis.kv_{db} (key, value, expires_at) \
                     VALUES ($1, $2, now() + ($3::bigint * interval '1 millisecond')) \
                     ON CONFLICT (key) DO UPDATE \
                     SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at"
                );
                let args: &[DatumWithOid] =
                    &[key.as_str().into(), value.as_str().into(), (*ex_ms).into()];
                match client.update(&sql, None, args) {
                    Ok(_) => Response::Ok,
                    Err(e) => {
                        eprintln!("pg_redis: PSETEX error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::MGet { keys } => {
                // Lazy expiry: purge expired keys for this batch before reading.
                let keys_vec: Vec<Option<String>> =
                    keys.iter().map(|k| Some(k.clone())).collect();
                client
                    .update(
                        &format!(
                            "DELETE FROM redis.kv_{db} \
                             WHERE key = ANY($1::text[]) AND expires_at <= now()"
                        ),
                        None,
                        &[keys_vec.clone().into()],
                    )
                    .ok();
                let sql = format!(
                    "SELECT key, value FROM redis.kv_{db} \
                     WHERE key = ANY($1::text[]) \
                     AND (expires_at IS NULL OR expires_at > now())"
                );
                match client.select(&sql, None, &[keys_vec.into()]) {
                    Ok(tbl) => {
                        let mut map = std::collections::HashMap::new();
                        for row in tbl {
                            if let (Ok(Some(k)), Ok(Some(v))) =
                                (row.get::<String>(1), row.get::<String>(2))
                            {
                                map.insert(k, v);
                            }
                        }
                        let result = keys
                            .iter()
                            .map(|k| map.remove(k).map(|v| v.into_bytes()))
                            .collect();
                        Response::Array(result)
                    }
                    Err(e) => {
                        eprintln!("pg_redis: MGET error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::MSet { pairs } => {
                let sql = format!(
                    "INSERT INTO redis.kv_{db} (key, value, expires_at) \
                     SELECT unnest($1::text[]), unnest($2::text[]), NULL \
                     ON CONFLICT (key) DO UPDATE \
                     SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at"
                );
                let keys: Vec<Option<String>> =
                    pairs.iter().map(|(k, _)| Some(k.clone())).collect();
                let vals: Vec<Option<String>> =
                    pairs.iter().map(|(_, v)| Some(v.clone())).collect();
                match client.update(&sql, None, &[keys.into(), vals.into()]) {
                    Ok(_) => Response::Ok,
                    Err(e) => {
                        eprintln!("pg_redis: MSET error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::Del { keys } => {
                let sql = format!(
                    "DELETE FROM redis.kv_{db} WHERE key = ANY($1::text[])"
                );
                let keys_vec: Vec<Option<String>> =
                    keys.iter().map(|k| Some(k.clone())).collect();
                match client.update(&sql, None, &[keys_vec.into()]) {
                    Ok(tbl) => Response::Integer(tbl.len() as i64),
                    Err(e) => {
                        eprintln!("pg_redis: DEL error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::Exists { keys } => {
                let sql = format!(
                    "SELECT count(*)::bigint FROM redis.kv_{db} \
                     WHERE key = ANY($1::text[]) \
                     AND (expires_at IS NULL OR expires_at > now())"
                );
                let keys_vec: Vec<Option<String>> =
                    keys.iter().map(|k| Some(k.clone())).collect();
                match client.select(&sql, None, &[keys_vec.into()]) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Integer(0),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: EXISTS error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::Expire { key, secs } => update_expiry(
                client,
                &format!(
                    "UPDATE redis.kv_{db} \
                     SET expires_at = now() + ($2::bigint * interval '1 second') \
                     WHERE key = $1"
                ),
                key,
                *secs,
            ),

            Command::PExpire { key, ms } => update_expiry(
                client,
                &format!(
                    "UPDATE redis.kv_{db} \
                     SET expires_at = now() + ($2::bigint * interval '1 millisecond') \
                     WHERE key = $1"
                ),
                key,
                *ms,
            ),

            Command::ExpireAt { key, unix_secs } => update_expiry(
                client,
                &format!(
                    "UPDATE redis.kv_{db} \
                     SET expires_at = to_timestamp($2::bigint) WHERE key = $1"
                ),
                key,
                *unix_secs,
            ),

            Command::PExpireAt { key, unix_ms } => {
                let sql = format!(
                    "UPDATE redis.kv_{db} \
                     SET expires_at = to_timestamp($2::float8 / 1000.0) WHERE key = $1"
                );
                let ms_f = *unix_ms as f64;
                match client.update(&sql, None, &[key.as_str().into(), ms_f.into()]) {
                    Ok(tbl) => Response::Integer(if !tbl.is_empty() { 1 } else { 0 }),
                    Err(e) => {
                        eprintln!("pg_redis: PEXPIREAT error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            // TTL variants use a single LEFT JOIN instead of three correlated subqueries,
            // reducing index scans from 3 to 1 per call.
            Command::Ttl { key } => get_ttl(
                client,
                &format!(
                    "SELECT CASE \
                       WHEN r.key IS NULL THEN -2::bigint \
                       WHEN r.expires_at IS NULL THEN -1::bigint \
                       ELSE GREATEST(-1, EXTRACT(EPOCH FROM (r.expires_at - now()))::bigint) \
                     END \
                     FROM (VALUES ($1::text)) AS dummy(k) \
                     LEFT JOIN redis.kv_{db} r ON r.key = dummy.k"
                ),
                key,
            ),

            Command::PTtl { key } => get_ttl(
                client,
                &format!(
                    "SELECT CASE \
                       WHEN r.key IS NULL THEN -2::bigint \
                       WHEN r.expires_at IS NULL THEN -1::bigint \
                       ELSE GREATEST(-1, (EXTRACT(EPOCH FROM (r.expires_at - now())) * 1000)::bigint) \
                     END \
                     FROM (VALUES ($1::text)) AS dummy(k) \
                     LEFT JOIN redis.kv_{db} r ON r.key = dummy.k"
                ),
                key,
            ),

            Command::Persist { key } => {
                let sql = format!(
                    "UPDATE redis.kv_{db} SET expires_at = NULL \
                     WHERE key = $1 AND expires_at IS NOT NULL"
                );
                match client.update(&sql, None, &[key.as_str().into()]) {
                    Ok(tbl) => Response::Integer(if !tbl.is_empty() { 1 } else { 0 }),
                    Err(e) => {
                        eprintln!("pg_redis: PERSIST error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::ExpireTime { key } => get_ttl(
                client,
                &format!(
                    "SELECT CASE \
                       WHEN r.key IS NULL THEN -2::bigint \
                       WHEN r.expires_at IS NULL THEN -1::bigint \
                       ELSE EXTRACT(EPOCH FROM r.expires_at)::bigint \
                     END \
                     FROM (VALUES ($1::text)) AS dummy(k) \
                     LEFT JOIN redis.kv_{db} r ON r.key = dummy.k"
                ),
                key,
            ),

            Command::PExpireTime { key } => get_ttl(
                client,
                &format!(
                    "SELECT CASE \
                       WHEN r.key IS NULL THEN -2::bigint \
                       WHEN r.expires_at IS NULL THEN -1::bigint \
                       ELSE (EXTRACT(EPOCH FROM r.expires_at) * 1000)::bigint \
                     END \
                     FROM (VALUES ($1::text)) AS dummy(k) \
                     LEFT JOIN redis.kv_{db} r ON r.key = dummy.k"
                ),
                key,
            ),

            Command::Incr { key } => {
                let sql = format!(
                    "INSERT INTO redis.kv_{db} (key, value) VALUES ($1, '1') \
                     ON CONFLICT (key) DO UPDATE \
                     SET value = (CAST(redis.kv_{db}.value AS bigint) + 1)::text \
                     RETURNING value::bigint"
                );
                match client.update(&sql, None, &[key.as_str().into()]) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Integer(1),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: INCR error: {}", e);
                        Response::Error(
                            "ERR value is not an integer or out of range".to_string(),
                        )
                    }
                }
            }

            Command::Decr { key } => {
                let sql = format!(
                    "INSERT INTO redis.kv_{db} (key, value) VALUES ($1, '-1') \
                     ON CONFLICT (key) DO UPDATE \
                     SET value = (CAST(redis.kv_{db}.value AS bigint) - 1)::text \
                     RETURNING value::bigint"
                );
                match client.update(&sql, None, &[key.as_str().into()]) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Integer(-1),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: DECR error: {}", e);
                        Response::Error(
                            "ERR value is not an integer or out of range".to_string(),
                        )
                    }
                }
            }

            Command::IncrBy { key, delta } => {
                let sql = format!(
                    "INSERT INTO redis.kv_{db} (key, value) VALUES ($1, $2::text) \
                     ON CONFLICT (key) DO UPDATE \
                     SET value = (CAST(redis.kv_{db}.value AS bigint) + $3)::text \
                     RETURNING value::bigint"
                );
                match client.update(&sql, None, &[key.as_str().into(), (*delta).into(), (*delta).into()]) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Integer(*delta),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: INCRBY error: {}", e);
                        Response::Error(
                            "ERR value is not an integer or out of range".to_string(),
                        )
                    }
                }
            }

            Command::DecrBy { key, delta } => {
                let neg_delta = -*delta;
                let sql = format!(
                    "INSERT INTO redis.kv_{db} (key, value) VALUES ($1, $2::text) \
                     ON CONFLICT (key) DO UPDATE \
                     SET value = (CAST(redis.kv_{db}.value AS bigint) + $3)::text \
                     RETURNING value::bigint"
                );
                match client.update(&sql, None, &[key.as_str().into(), neg_delta.into(), neg_delta.into()]) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Integer(neg_delta),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: DECRBY error: {}", e);
                        Response::Error(
                            "ERR value is not an integer or out of range".to_string(),
                        )
                    }
                }
            }

            Command::IncrByFloat { key, delta } => {
                let sql = format!(
                    "INSERT INTO redis.kv_{db} (key, value) VALUES ($1, $2::text) \
                     ON CONFLICT (key) DO UPDATE \
                     SET value = (CAST(redis.kv_{db}.value AS float8) + $3)::text \
                     RETURNING value"
                );
                match client.update(&sql, None, &[key.as_str().into(), (*delta).into(), (*delta).into()]) {
                    Ok(tbl) => match tbl.first().get::<String>(1) {
                        Ok(Some(v)) => Response::BulkString(v.into_bytes()),
                        _ => Response::BulkString(delta.to_string().into_bytes()),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: INCRBYFLOAT error: {}", e);
                        Response::Error("ERR value is not a valid float".to_string())
                    }
                }
            }

            Command::Append { key, value } => {
                let sql = format!(
                    "INSERT INTO redis.kv_{db} (key, value) VALUES ($1, $2) \
                     ON CONFLICT (key) DO UPDATE \
                     SET value = redis.kv_{db}.value || $2 \
                     RETURNING length(redis.kv_{db}.value)"
                );
                match client.update(&sql, None, &[key.as_str().into(), value.as_str().into()]) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Integer(value.len() as i64),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: APPEND error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::Strlen { key } => {
                let sql = format!(
                    "SELECT length(value) FROM redis.kv_{db} \
                     WHERE key = $1 AND (expires_at IS NULL OR expires_at > now())"
                );
                match client.select(&sql, None, &[key.as_str().into()]) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Integer(0),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: STRLEN error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::GetDel { key } => {
                let sql = format!(
                    "WITH deleted AS (DELETE FROM redis.kv_{db} WHERE key = $1 \
                     AND (expires_at IS NULL OR expires_at > now()) RETURNING value) \
                     SELECT value FROM deleted"
                );
                match client.update(&sql, None, &[key.as_str().into()]) {
                    Ok(tbl) => match tbl.first().get::<String>(1) {
                        Ok(Some(v)) => Response::BulkString(v.into_bytes()),
                        _ => Response::Null,
                    },
                    Err(e) => {
                        eprintln!("pg_redis: GETDEL error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::GetSet { key, value } => {
                let sql = format!(
                    "INSERT INTO redis.kv_{db} (key, value) VALUES ($1, $2) \
                     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value \
                     RETURNING (SELECT value FROM redis.kv_{db} WHERE key = $1)"
                );
                match client.update(&sql, None, &[key.as_str().into(), value.as_str().into()]) {
                    Ok(tbl) => match tbl.first().get::<String>(1) {
                        Ok(Some(v)) => Response::BulkString(v.into_bytes()),
                        _ => Response::Null,
                    },
                    Err(e) => {
                        eprintln!("pg_redis: GETSET error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::SetNx { key, value } => {
                let sql = format!(
                    "INSERT INTO redis.kv_{db} (key, value) VALUES ($1, $2) \
                     ON CONFLICT (key) DO NOTHING"
                );
                match client.update(&sql, None, &[key.as_str().into(), value.as_str().into()]) {
                    Ok(tbl) => Response::Integer(if !tbl.is_empty() { 1 } else { 0 }),
                    Err(e) => {
                        eprintln!("pg_redis: SETNX error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::MSetNx { pairs } => {
                let keys: Vec<Option<String>> =
                    pairs.iter().map(|(k, _)| Some(k.clone())).collect();
                let check_sql = format!(
                    "SELECT count(*) FROM redis.kv_{db} \
                     WHERE key = ANY($1::text[]) \
                     AND (expires_at IS NULL OR expires_at > now())"
                );
                let existing = client.select(&check_sql, None, &[keys.clone().into()]);
                let any_exist = match existing {
                    Ok(tbl) => matches!(tbl.first().get::<i64>(1), Ok(Some(n)) if n > 0),
                    Err(_) => false,
                };
                if any_exist {
                    return Response::Integer(0);
                }
                let vals: Vec<Option<String>> =
                    pairs.iter().map(|(_, v)| Some(v.clone())).collect();
                let insert_sql = format!(
                    "INSERT INTO redis.kv_{db} (key, value, expires_at) \
                     SELECT unnest($1::text[]), unnest($2::text[]), NULL \
                     ON CONFLICT (key) DO NOTHING"
                );
                match client.update(&insert_sql, None, &[keys.into(), vals.into()]) {
                    Ok(_) => Response::Integer(1),
                    Err(e) => {
                        eprintln!("pg_redis: MSETNX error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::Type { key } => {
                let kv_sql = format!(
                    "SELECT 1 FROM redis.kv_{db} WHERE key = $1 \
                     AND (expires_at IS NULL OR expires_at > now())"
                );
                match client.select(&kv_sql, None, &[key.as_str().into()]) {
                    Ok(tbl) if !tbl.is_empty() => {
                        return Response::SimpleString("string".to_string());
                    }
                    _ => {}
                }
                let hash_sql = format!(
                    "SELECT 1 FROM redis.hash_{db} WHERE key = $1 LIMIT 1"
                );
                match client.select(&hash_sql, None, &[key.as_str().into()]) {
                    Ok(tbl) if !tbl.is_empty() => Response::SimpleString("hash".to_string()),
                    _ => Response::SimpleString("none".to_string()),
                }
            }

            Command::Keys { pattern } => {
                let sql_pattern = glob_to_sql_like(pattern);
                let kv_sql = format!(
                    "SELECT key FROM redis.kv_{db} \
                     WHERE key LIKE $1 AND (expires_at IS NULL OR expires_at > now())"
                );
                let hash_sql = format!(
                    "SELECT DISTINCT key FROM redis.hash_{db} WHERE key LIKE $1"
                );
                let mut keys: Vec<Option<Vec<u8>>> = Vec::new();
                if let Ok(tbl) = client.select(&kv_sql, None, &[sql_pattern.as_str().into()]) {
                    for row in tbl {
                        if let Ok(Some(k)) = row.get::<String>(1) {
                            keys.push(Some(k.into_bytes()));
                        }
                    }
                }
                if let Ok(tbl) = client.select(&hash_sql, None, &[sql_pattern.as_str().into()]) {
                    for row in tbl {
                        if let Ok(Some(k)) = row.get::<String>(1) {
                            keys.push(Some(k.into_bytes()));
                        }
                    }
                }
                Response::Array(keys)
            }

            Command::DbSize => {
                let kv_sql = format!(
                    "SELECT count(*)::bigint FROM redis.kv_{db} \
                     WHERE (expires_at IS NULL OR expires_at > now())"
                );
                let hash_sql = format!(
                    "SELECT count(DISTINCT key)::bigint FROM redis.hash_{db}"
                );
                let kv_count = match client.select(&kv_sql, None, &[]) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => n,
                        _ => 0,
                    },
                    Err(_) => 0,
                };
                let hash_count = match client.select(&hash_sql, None, &[]) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => n,
                        _ => 0,
                    },
                    Err(_) => 0,
                };
                Response::Integer(kv_count + hash_count)
            }

            Command::Unlink { keys } => {
                let sql = format!(
                    "DELETE FROM redis.kv_{db} WHERE key = ANY($1::text[])"
                );
                let keys_vec: Vec<Option<String>> =
                    keys.iter().map(|k| Some(k.clone())).collect();
                match client.update(&sql, None, &[keys_vec.into()]) {
                    Ok(tbl) => Response::Integer(tbl.len() as i64),
                    Err(e) => {
                        eprintln!("pg_redis: UNLINK error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::Rename { key, newkey } => {
                let check_sql = format!(
                    "SELECT value, expires_at FROM redis.kv_{db} WHERE key = $1"
                );
                match client.select(&check_sql, None, &[key.as_str().into()]) {
                    Ok(tbl) => {
                        if tbl.is_empty() {
                            return Response::Error("ERR no such key".to_string());
                        }
                    }
                    Err(e) => {
                        eprintln!("pg_redis: RENAME error: {}", e);
                        return Response::Error("internal error".to_string());
                    }
                }
                let sql = format!(
                    "UPDATE redis.kv_{db} SET key = $2 WHERE key = $1"
                );
                match client.update(&sql, None, &[key.as_str().into(), newkey.as_str().into()]) {
                    Ok(_) => Response::Ok,
                    Err(e) => {
                        eprintln!("pg_redis: RENAME error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::RandomKey => {
                let sql = format!(
                    "SELECT key FROM redis.kv_{db} \
                     WHERE (expires_at IS NULL OR expires_at > now()) \
                     ORDER BY random() LIMIT 1"
                );
                match client.select(&sql, None, &[]) {
                    Ok(tbl) => match tbl.first().get::<String>(1) {
                        Ok(Some(k)) => Response::BulkString(k.into_bytes()),
                        _ => Response::Null,
                    },
                    Err(e) => {
                        eprintln!("pg_redis: RANDOMKEY error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::Scan { pattern, .. } => {
                let sql_pattern = pattern
                    .as_deref()
                    .map(glob_to_sql_like)
                    .unwrap_or_else(|| "%".to_string());
                let kv_sql = format!(
                    "SELECT key FROM redis.kv_{db} \
                     WHERE key LIKE $1 AND (expires_at IS NULL OR expires_at > now())"
                );
                let hash_sql = format!(
                    "SELECT DISTINCT key FROM redis.hash_{db} WHERE key LIKE $1"
                );
                let mut keys: Vec<Option<Vec<u8>>> = Vec::new();
                if let Ok(tbl) = client.select(&kv_sql, None, &[sql_pattern.as_str().into()]) {
                    for row in tbl {
                        if let Ok(Some(k)) = row.get::<String>(1) {
                            keys.push(Some(k.into_bytes()));
                        }
                    }
                }
                if let Ok(tbl) = client.select(&hash_sql, None, &[sql_pattern.as_str().into()]) {
                    for row in tbl {
                        if let Ok(Some(k)) = row.get::<String>(1) {
                            keys.push(Some(k.into_bytes()));
                        }
                    }
                }
                Response::ScanResult { keys }
            }

            Command::HGet { key, field } => {
                let sql = format!(
                    "SELECT value FROM redis.hash_{db} WHERE key = $1 AND field = $2"
                );
                match client.select(&sql, None, &[key.as_str().into(), field.as_str().into()]) {
                    Ok(tbl) => match tbl.first().get::<String>(1) {
                        Ok(Some(v)) => Response::BulkString(v.into_bytes()),
                        _ => Response::Null,
                    },
                    Err(e) => {
                        eprintln!("pg_redis: HGET error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::HSet { key, pairs } => {
                let sql = format!(
                    "INSERT INTO redis.hash_{db} (key, field, value) \
                     SELECT $1, unnest($2::text[]), unnest($3::text[]) \
                     ON CONFLICT (key, field) DO UPDATE SET value = EXCLUDED.value"
                );
                let fields: Vec<Option<String>> =
                    pairs.iter().map(|(f, _)| Some(f.clone())).collect();
                let vals: Vec<Option<String>> =
                    pairs.iter().map(|(_, v)| Some(v.clone())).collect();
                match client.update(
                    &sql,
                    None,
                    &[key.as_str().into(), fields.into(), vals.into()],
                ) {
                    Ok(tbl) => Response::Integer(tbl.len() as i64),
                    Err(e) => {
                        eprintln!("pg_redis: HSET error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::HDel { key, fields } => {
                let sql = format!(
                    "DELETE FROM redis.hash_{db} \
                     WHERE key = $1 AND field = ANY($2::text[])"
                );
                let fields_vec: Vec<Option<String>> =
                    fields.iter().map(|f| Some(f.clone())).collect();
                match client.update(&sql, None, &[key.as_str().into(), fields_vec.into()]) {
                    Ok(tbl) => Response::Integer(tbl.len() as i64),
                    Err(e) => {
                        eprintln!("pg_redis: HDEL error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::HGetAll { key } => {
                let sql = format!(
                    "SELECT field, value FROM redis.hash_{db} \
                     WHERE key = $1 ORDER BY field"
                );
                match client.select(&sql, None, &[key.as_str().into()]) {
                    Ok(tbl) => {
                        let mut items: Vec<Option<Vec<u8>>> = Vec::new();
                        for row in tbl {
                            if let (Ok(Some(f)), Ok(Some(v))) =
                                (row.get::<String>(1), row.get::<String>(2))
                            {
                                items.push(Some(f.into_bytes()));
                                items.push(Some(v.into_bytes()));
                            }
                        }
                        Response::Array(items)
                    }
                    Err(e) => {
                        eprintln!("pg_redis: HGETALL error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::HMGet { key, fields } => {
                let fields_vec: Vec<Option<String>> =
                    fields.iter().map(|f| Some(f.clone())).collect();
                let sql = format!(
                    "SELECT field, value FROM redis.hash_{db} \
                     WHERE key = $1 AND field = ANY($2::text[])"
                );
                match client.select(&sql, None, &[key.as_str().into(), fields_vec.into()]) {
                    Ok(tbl) => {
                        let mut map = std::collections::HashMap::new();
                        for row in tbl {
                            if let (Ok(Some(f)), Ok(Some(v))) =
                                (row.get::<String>(1), row.get::<String>(2))
                            {
                                map.insert(f, v);
                            }
                        }
                        let result = fields
                            .iter()
                            .map(|f| map.remove(f).map(|v| v.into_bytes()))
                            .collect();
                        Response::Array(result)
                    }
                    Err(e) => {
                        eprintln!("pg_redis: HMGET error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::HMSet { key, pairs } => {
                let sql = format!(
                    "INSERT INTO redis.hash_{db} (key, field, value) \
                     SELECT $1, unnest($2::text[]), unnest($3::text[]) \
                     ON CONFLICT (key, field) DO UPDATE SET value = EXCLUDED.value"
                );
                let fields: Vec<Option<String>> =
                    pairs.iter().map(|(f, _)| Some(f.clone())).collect();
                let vals: Vec<Option<String>> =
                    pairs.iter().map(|(_, v)| Some(v.clone())).collect();
                match client.update(
                    &sql,
                    None,
                    &[key.as_str().into(), fields.into(), vals.into()],
                ) {
                    Ok(_) => Response::Ok,
                    Err(e) => {
                        eprintln!("pg_redis: HMSET error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::HKeys { key } => {
                let sql = format!(
                    "SELECT field FROM redis.hash_{db} WHERE key = $1 ORDER BY field"
                );
                match client.select(&sql, None, &[key.as_str().into()]) {
                    Ok(tbl) => {
                        let items = tbl
                            .into_iter()
                            .filter_map(|row| row.get::<String>(1).ok().flatten())
                            .map(|f| Some(f.into_bytes()))
                            .collect();
                        Response::Array(items)
                    }
                    Err(e) => {
                        eprintln!("pg_redis: HKEYS error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::HVals { key } => {
                let sql = format!(
                    "SELECT value FROM redis.hash_{db} WHERE key = $1 ORDER BY field"
                );
                match client.select(&sql, None, &[key.as_str().into()]) {
                    Ok(tbl) => {
                        let items = tbl
                            .into_iter()
                            .filter_map(|row| row.get::<String>(1).ok().flatten())
                            .map(|v| Some(v.into_bytes()))
                            .collect();
                        Response::Array(items)
                    }
                    Err(e) => {
                        eprintln!("pg_redis: HVALS error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::HExists { key, field } => {
                let sql = format!(
                    "SELECT 1 FROM redis.hash_{db} WHERE key = $1 AND field = $2"
                );
                match client.select(&sql, None, &[key.as_str().into(), field.as_str().into()]) {
                    Ok(tbl) => Response::Integer(if !tbl.is_empty() { 1 } else { 0 }),
                    Err(e) => {
                        eprintln!("pg_redis: HEXISTS error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::HLen { key } => {
                let sql = format!(
                    "SELECT count(*)::bigint FROM redis.hash_{db} WHERE key = $1"
                );
                match client.select(&sql, None, &[key.as_str().into()]) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Integer(0),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: HLEN error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }

            Command::HIncrBy { key, field, delta } => {
                let sql = format!(
                    "INSERT INTO redis.hash_{db} (key, field, value) VALUES ($1, $2, $3::text) \
                     ON CONFLICT (key, field) DO UPDATE \
                     SET value = (CAST(redis.hash_{db}.value AS bigint) + $4)::text \
                     RETURNING value::bigint"
                );
                match client.update(
                    &sql,
                    None,
                    &[key.as_str().into(), field.as_str().into(), (*delta).into(), (*delta).into()],
                ) {
                    Ok(tbl) => match tbl.first().get::<i64>(1) {
                        Ok(Some(n)) => Response::Integer(n),
                        _ => Response::Integer(*delta),
                    },
                    Err(e) => {
                        eprintln!("pg_redis: HINCRBY error: {}", e);
                        Response::Error(
                            "ERR value is not an integer or out of range".to_string(),
                        )
                    }
                }
            }

            Command::HSetNx { key, field, value } => {
                let sql = format!(
                    "INSERT INTO redis.hash_{db} (key, field, value) VALUES ($1, $2, $3) \
                     ON CONFLICT (key, field) DO NOTHING"
                );
                match client.update(
                    &sql,
                    None,
                    &[key.as_str().into(), field.as_str().into(), value.as_str().into()],
                ) {
                    Ok(tbl) => Response::Integer(if !tbl.is_empty() { 1 } else { 0 }),
                    Err(e) => {
                        eprintln!("pg_redis: HSETNX error: {}", e);
                        Response::Error("internal error".to_string())
                    }
                }
            }
        }
    }
}

fn str_arg(args: &[Vec<u8>], idx: usize, cmd: &str) -> Result<String, String> {
    args.get(idx)
        .map(|a| String::from_utf8_lossy(a).into_owned())
        .ok_or_else(|| format!("{} missing argument at position {}", cmd, idx))
}

fn update_expiry(client: &mut SpiClient<'_>, sql: &str, key: &str, n: i64) -> Response {
    match client.update(sql, None, &[key.into(), n.into()]) {
        Ok(tbl) => Response::Integer(if !tbl.is_empty() { 1 } else { 0 }),
        Err(e) => {
            eprintln!("pg_redis: expiry update error: {}", e);
            Response::Error("internal error".to_string())
        }
    }
}

fn get_ttl(client: &mut SpiClient<'_>, sql: &str, key: &str) -> Response {
    match client.select(sql, None, &[key.into()]) {
        Ok(tbl) => match tbl.first().get::<i64>(1) {
            Ok(Some(n)) => Response::Integer(n),
            _ => Response::Integer(-2),
        },
        Err(e) => {
            eprintln!("pg_redis: TTL error: {}", e);
            Response::Error("internal error".to_string())
        }
    }
}

fn glob_to_sql_like(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len() + 4);
    for ch in pattern.chars() {
        match ch {
            '*' => out.push('%'),
            '?' => out.push('_'),
            '%' => out.push_str("\\%"),
            '_' => out.push_str("\\_"),
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parts(input: &[&str]) -> Vec<Vec<u8>> {
        input.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    fn is_ok(cmd: Result<Command, String>) -> Command {
        assert!(cmd.is_ok(), "expected Ok, got Err: {:?}", cmd.err());
        cmd.unwrap()
    }

    fn is_err(cmd: Result<Command, String>) -> String {
        assert!(cmd.is_err(), "expected Err but got Ok");
        cmd.unwrap_err()
    }

    // ──────────────────────── Connection commands ────────────────────────────

    #[test]
    fn parse_ping_no_arg() {
        let cmd = is_ok(Command::parse(parts(&["PING"])));
        assert!(matches!(cmd, Command::Ping { msg: None }));
    }

    #[test]
    fn parse_ping_with_arg() {
        let cmd = is_ok(Command::parse(parts(&["PING", "hello"])));
        assert!(matches!(cmd, Command::Ping { msg: Some(_) }));
    }

    #[test]
    fn parse_echo() {
        let cmd = is_ok(Command::parse(parts(&["ECHO", "world"])));
        assert!(matches!(cmd, Command::Echo { .. }));
    }

    #[test]
    fn parse_echo_missing_arg() {
        is_err(Command::parse(parts(&["ECHO"])));
    }

    #[test]
    fn parse_select_zero() {
        let cmd = is_ok(Command::parse(parts(&["SELECT", "0"])));
        assert!(matches!(cmd, Command::Select { db: 0 }));
    }

    #[test]
    fn parse_select_one() {
        let cmd = is_ok(Command::parse(parts(&["SELECT", "1"])));
        assert!(matches!(cmd, Command::Select { db: 1 }));
    }

    #[test]
    fn parse_select_invalid() {
        is_err(Command::parse(parts(&["SELECT", "abc"])));
    }

    #[test]
    fn parse_auth_single_arg() {
        let cmd = is_ok(Command::parse(parts(&["AUTH", "secret"])));
        assert!(matches!(cmd, Command::Auth { password } if password == "secret"));
    }

    #[test]
    fn parse_auth_acl_form_uses_last_arg_as_password() {
        let cmd = is_ok(Command::parse(parts(&["AUTH", "user", "mypass"])));
        assert!(matches!(cmd, Command::Auth { password } if password == "mypass"));
    }

    #[test]
    fn parse_info() {
        let cmd = is_ok(Command::parse(parts(&["INFO"])));
        assert!(matches!(cmd, Command::Info));
    }

    // ──────────────────────── CLIENT subcommand parsing ──────────────────────

    #[test]
    fn parse_client_id() {
        let cmd = is_ok(Command::parse(parts(&["CLIENT", "ID"])));
        assert!(matches!(cmd, Command::ClientId));
    }

    #[test]
    fn parse_client_getname() {
        let cmd = is_ok(Command::parse(parts(&["CLIENT", "GETNAME"])));
        assert!(matches!(cmd, Command::ClientGetname));
    }

    #[test]
    fn parse_client_setname() {
        let cmd = is_ok(Command::parse(parts(&["CLIENT", "SETNAME", "myconn"])));
        assert!(matches!(cmd, Command::ClientSetname { name } if name == "myconn"));
    }

    #[test]
    fn parse_client_setname_missing_arg() {
        is_err(Command::parse(parts(&["CLIENT", "SETNAME"])));
    }

    #[test]
    fn parse_client_setinfo() {
        let cmd = is_ok(Command::parse(parts(&["CLIENT", "SETINFO", "lib-name", "redis-rs"])));
        assert!(matches!(cmd, Command::ClientSetinfo));
    }

    #[test]
    fn parse_client_list() {
        let cmd = is_ok(Command::parse(parts(&["CLIENT", "LIST"])));
        assert!(matches!(cmd, Command::ClientList));
    }

    #[test]
    fn parse_client_info() {
        let cmd = is_ok(Command::parse(parts(&["CLIENT", "INFO"])));
        assert!(matches!(cmd, Command::ClientInfo));
    }

    #[test]
    fn parse_client_no_evict() {
        let cmd = is_ok(Command::parse(parts(&["CLIENT", "NO-EVICT", "on"])));
        assert!(matches!(cmd, Command::ClientNoEvict));
    }

    #[test]
    fn parse_client_no_touch() {
        let cmd = is_ok(Command::parse(parts(&["CLIENT", "NO-TOUCH", "on"])));
        assert!(matches!(cmd, Command::ClientNoTouch));
    }

    #[test]
    fn parse_client_unknown_subcommand() {
        let cmd = is_ok(Command::parse(parts(&["CLIENT", "UNPAUSE"])));
        assert!(matches!(cmd, Command::ClientOther));
    }

    #[test]
    fn parse_client_no_subcommand() {
        let cmd = is_ok(Command::parse(parts(&["CLIENT"])));
        assert!(matches!(cmd, Command::ClientOther));
    }

    // ──────────────────────── COMMAND subcommand parsing ─────────────────────

    #[test]
    fn parse_command_bare() {
        let cmd = is_ok(Command::parse(parts(&["COMMAND"])));
        assert!(matches!(cmd, Command::CommandOther));
    }

    #[test]
    fn parse_command_count() {
        let cmd = is_ok(Command::parse(parts(&["COMMAND", "COUNT"])));
        assert!(matches!(cmd, Command::CommandCount));
    }

    #[test]
    fn parse_command_info() {
        let cmd = is_ok(Command::parse(parts(&["COMMAND", "INFO", "get"])));
        assert!(matches!(cmd, Command::CommandInfo));
    }

    #[test]
    fn parse_command_docs() {
        let cmd = is_ok(Command::parse(parts(&["COMMAND", "DOCS"])));
        assert!(matches!(cmd, Command::CommandDocs));
    }

    #[test]
    fn parse_command_list() {
        let cmd = is_ok(Command::parse(parts(&["COMMAND", "LIST"])));
        assert!(matches!(cmd, Command::CommandList));
    }

    // ──────────────────────── CONFIG subcommand parsing ──────────────────────

    #[test]
    fn parse_config_get() {
        let cmd = is_ok(Command::parse(parts(&["CONFIG", "GET", "maxmemory"])));
        assert!(matches!(cmd, Command::ConfigGet { pattern } if pattern == "maxmemory"));
    }

    #[test]
    fn parse_config_get_missing_pattern() {
        is_err(Command::parse(parts(&["CONFIG", "GET"])));
    }

    #[test]
    fn parse_config_set() {
        let cmd = is_ok(Command::parse(parts(&["CONFIG", "SET", "maxmemory", "100mb"])));
        assert!(matches!(cmd, Command::ConfigSet));
    }

    #[test]
    fn parse_config_other() {
        let cmd = is_ok(Command::parse(parts(&["CONFIG", "RESETSTAT"])));
        assert!(matches!(cmd, Command::ConfigOther));
    }

    // ─────────────────────────── Key-value commands ──────────────────────────

    #[test]
    fn parse_get() {
        let cmd = is_ok(Command::parse(parts(&["GET", "mykey"])));
        assert!(matches!(cmd, Command::Get { key } if key == "mykey"));
    }

    #[test]
    fn parse_get_missing_key() {
        is_err(Command::parse(parts(&["GET"])));
    }

    #[test]
    fn parse_set_no_expiry() {
        let cmd = is_ok(Command::parse(parts(&["SET", "k", "v"])));
        assert!(
            matches!(cmd, Command::Set { key, value, ex_ms: None } if key == "k" && value == "v")
        );
    }

    #[test]
    fn parse_set_with_ex() {
        let cmd = is_ok(Command::parse(parts(&["SET", "k", "v", "EX", "10"])));
        assert!(matches!(cmd, Command::Set { ex_ms: Some(10000), .. }));
    }

    #[test]
    fn parse_set_with_px() {
        let cmd = is_ok(Command::parse(parts(&["SET", "k", "v", "PX", "500"])));
        assert!(matches!(cmd, Command::Set { ex_ms: Some(500), .. }));
    }

    #[test]
    fn parse_set_case_insensitive_ex() {
        let cmd = is_ok(Command::parse(parts(&["set", "k", "v", "ex", "5"])));
        assert!(matches!(cmd, Command::Set { ex_ms: Some(5000), .. }));
    }

    #[test]
    fn parse_setex() {
        let cmd = is_ok(Command::parse(parts(&["SETEX", "k", "30", "v"])));
        assert!(matches!(cmd, Command::SetEx { ex_secs: 30, .. }));
    }

    #[test]
    fn parse_psetex() {
        let cmd = is_ok(Command::parse(parts(&["PSETEX", "k", "300", "v"])));
        assert!(matches!(cmd, Command::PSetEx { ex_ms: 300, .. }));
    }

    #[test]
    fn parse_mget() {
        let cmd = is_ok(Command::parse(parts(&["MGET", "k1", "k2", "k3"])));
        assert!(matches!(cmd, Command::MGet { keys } if keys.len() == 3));
    }

    #[test]
    fn parse_mget_missing_keys() {
        is_err(Command::parse(parts(&["MGET"])));
    }

    #[test]
    fn parse_mset() {
        let cmd = is_ok(Command::parse(parts(&["MSET", "k1", "v1", "k2", "v2"])));
        assert!(matches!(cmd, Command::MSet { pairs } if pairs.len() == 2));
    }

    #[test]
    fn parse_mset_odd_args() {
        is_err(Command::parse(parts(&["MSET", "k1", "v1", "k2"])));
    }

    #[test]
    fn parse_del_single() {
        let cmd = is_ok(Command::parse(parts(&["DEL", "k"])));
        assert!(matches!(cmd, Command::Del { keys } if keys.len() == 1));
    }

    #[test]
    fn parse_del_multiple() {
        let cmd = is_ok(Command::parse(parts(&["DEL", "k1", "k2", "k3"])));
        assert!(matches!(cmd, Command::Del { keys } if keys.len() == 3));
    }

    #[test]
    fn parse_del_missing_key() {
        is_err(Command::parse(parts(&["DEL"])));
    }

    #[test]
    fn parse_exists() {
        let cmd = is_ok(Command::parse(parts(&["EXISTS", "k1", "k2"])));
        assert!(matches!(cmd, Command::Exists { keys } if keys.len() == 2));
    }

    // ──────────────────────── String increment commands ──────────────────────

    #[test]
    fn parse_incr() {
        let cmd = is_ok(Command::parse(parts(&["INCR", "counter"])));
        assert!(matches!(cmd, Command::Incr { key } if key == "counter"));
    }

    #[test]
    fn parse_incr_missing_key() {
        is_err(Command::parse(parts(&["INCR"])));
    }

    #[test]
    fn parse_decr() {
        let cmd = is_ok(Command::parse(parts(&["DECR", "counter"])));
        assert!(matches!(cmd, Command::Decr { key } if key == "counter"));
    }

    #[test]
    fn parse_incrby() {
        let cmd = is_ok(Command::parse(parts(&["INCRBY", "counter", "5"])));
        assert!(matches!(cmd, Command::IncrBy { key, delta } if key == "counter" && delta == 5));
    }

    #[test]
    fn parse_incrby_invalid_delta() {
        is_err(Command::parse(parts(&["INCRBY", "counter", "abc"])));
    }

    #[test]
    fn parse_decrby() {
        let cmd = is_ok(Command::parse(parts(&["DECRBY", "counter", "3"])));
        assert!(matches!(cmd, Command::DecrBy { key, delta } if key == "counter" && delta == 3));
    }

    #[test]
    fn parse_incrbyfloat() {
        let cmd = is_ok(Command::parse(parts(&["INCRBYFLOAT", "f", "1.5"])));
        assert!(matches!(cmd, Command::IncrByFloat { key, delta } if key == "f" && delta == 1.5));
    }

    #[test]
    fn parse_incrbyfloat_invalid() {
        is_err(Command::parse(parts(&["INCRBYFLOAT", "f", "notafloat"])));
    }

    #[test]
    fn parse_append() {
        let cmd = is_ok(Command::parse(parts(&["APPEND", "k", "suffix"])));
        assert!(matches!(cmd, Command::Append { key, value } if key == "k" && value == "suffix"));
    }

    #[test]
    fn parse_strlen() {
        let cmd = is_ok(Command::parse(parts(&["STRLEN", "k"])));
        assert!(matches!(cmd, Command::Strlen { key } if key == "k"));
    }

    #[test]
    fn parse_getdel() {
        let cmd = is_ok(Command::parse(parts(&["GETDEL", "k"])));
        assert!(matches!(cmd, Command::GetDel { key } if key == "k"));
    }

    #[test]
    fn parse_getset() {
        let cmd = is_ok(Command::parse(parts(&["GETSET", "k", "newval"])));
        assert!(matches!(cmd, Command::GetSet { key, value } if key == "k" && value == "newval"));
    }

    #[test]
    fn parse_setnx() {
        let cmd = is_ok(Command::parse(parts(&["SETNX", "k", "v"])));
        assert!(matches!(cmd, Command::SetNx { key, value } if key == "k" && value == "v"));
    }

    #[test]
    fn parse_msetnx() {
        let cmd = is_ok(Command::parse(parts(&["MSETNX", "k1", "v1", "k2", "v2"])));
        assert!(matches!(cmd, Command::MSetNx { pairs } if pairs.len() == 2));
    }

    #[test]
    fn parse_msetnx_odd_args() {
        is_err(Command::parse(parts(&["MSETNX", "k1"])));
    }

    // ─────────────────────────── Expiry commands ─────────────────────────────

    #[test]
    fn parse_expire() {
        let cmd = is_ok(Command::parse(parts(&["EXPIRE", "k", "60"])));
        assert!(matches!(cmd, Command::Expire { secs: 60, .. }));
    }

    #[test]
    fn parse_pexpire() {
        let cmd = is_ok(Command::parse(parts(&["PEXPIRE", "k", "5000"])));
        assert!(matches!(cmd, Command::PExpire { ms: 5000, .. }));
    }

    #[test]
    fn parse_expireat() {
        let cmd = is_ok(Command::parse(parts(&["EXPIREAT", "k", "1700000000"])));
        assert!(matches!(cmd, Command::ExpireAt { unix_secs: 1700000000, .. }));
    }

    #[test]
    fn parse_pexpireat() {
        let cmd = is_ok(Command::parse(parts(&["PEXPIREAT", "k", "1700000000000"])));
        assert!(matches!(cmd, Command::PExpireAt { unix_ms: 1700000000000, .. }));
    }

    #[test]
    fn parse_ttl() {
        let cmd = is_ok(Command::parse(parts(&["TTL", "k"])));
        assert!(matches!(cmd, Command::Ttl { .. }));
    }

    #[test]
    fn parse_pttl() {
        let cmd = is_ok(Command::parse(parts(&["PTTL", "k"])));
        assert!(matches!(cmd, Command::PTtl { .. }));
    }

    #[test]
    fn parse_persist() {
        let cmd = is_ok(Command::parse(parts(&["PERSIST", "k"])));
        assert!(matches!(cmd, Command::Persist { .. }));
    }

    #[test]
    fn parse_expiretime() {
        let cmd = is_ok(Command::parse(parts(&["EXPIRETIME", "k"])));
        assert!(matches!(cmd, Command::ExpireTime { .. }));
    }

    #[test]
    fn parse_pexpiretime() {
        let cmd = is_ok(Command::parse(parts(&["PEXPIRETIME", "k"])));
        assert!(matches!(cmd, Command::PExpireTime { .. }));
    }

    // ─────────────────────── Key inspection commands ──────────────────────────

    #[test]
    fn parse_type() {
        let cmd = is_ok(Command::parse(parts(&["TYPE", "k"])));
        assert!(matches!(cmd, Command::Type { key } if key == "k"));
    }

    #[test]
    fn parse_keys_star() {
        let cmd = is_ok(Command::parse(parts(&["KEYS", "*"])));
        assert!(matches!(cmd, Command::Keys { pattern } if pattern == "*"));
    }

    #[test]
    fn parse_keys_prefix() {
        let cmd = is_ok(Command::parse(parts(&["KEYS", "user:*"])));
        assert!(matches!(cmd, Command::Keys { pattern } if pattern == "user:*"));
    }

    #[test]
    fn parse_dbsize() {
        let cmd = is_ok(Command::parse(parts(&["DBSIZE"])));
        assert!(matches!(cmd, Command::DbSize));
    }

    #[test]
    fn parse_unlink() {
        let cmd = is_ok(Command::parse(parts(&["UNLINK", "k1", "k2"])));
        assert!(matches!(cmd, Command::Unlink { keys } if keys.len() == 2));
    }

    #[test]
    fn parse_unlink_missing_key() {
        is_err(Command::parse(parts(&["UNLINK"])));
    }

    #[test]
    fn parse_rename() {
        let cmd = is_ok(Command::parse(parts(&["RENAME", "old", "new"])));
        assert!(matches!(cmd, Command::Rename { key, newkey } if key == "old" && newkey == "new"));
    }

    #[test]
    fn parse_randomkey() {
        let cmd = is_ok(Command::parse(parts(&["RANDOMKEY"])));
        assert!(matches!(cmd, Command::RandomKey));
    }

    #[test]
    fn parse_scan_zero_cursor() {
        let cmd = is_ok(Command::parse(parts(&["SCAN", "0"])));
        assert!(matches!(cmd, Command::Scan { cursor: 0, pattern: None, count: None }));
    }

    #[test]
    fn parse_scan_with_match() {
        let cmd = is_ok(Command::parse(parts(&["SCAN", "0", "MATCH", "user:*"])));
        assert!(
            matches!(cmd, Command::Scan { cursor: 0, pattern: Some(p), count: None } if p == "user:*")
        );
    }

    #[test]
    fn parse_scan_with_count() {
        let cmd = is_ok(Command::parse(parts(&["SCAN", "0", "COUNT", "100"])));
        assert!(matches!(cmd, Command::Scan { cursor: 0, pattern: None, count: Some(100) }));
    }

    #[test]
    fn parse_scan_with_match_and_count() {
        let cmd = is_ok(Command::parse(parts(&["SCAN", "0", "MATCH", "*", "COUNT", "10"])));
        assert!(
            matches!(cmd, Command::Scan { cursor: 0, pattern: Some(_), count: Some(10) })
        );
    }

    #[test]
    fn parse_scan_invalid_cursor() {
        is_err(Command::parse(parts(&["SCAN", "notanumber"])));
    }

    // ─────────────────────────── Hash commands ───────────────────────────────

    #[test]
    fn parse_hget() {
        let cmd = is_ok(Command::parse(parts(&["HGET", "h", "f"])));
        assert!(matches!(cmd, Command::HGet { key, field } if key == "h" && field == "f"));
    }

    #[test]
    fn parse_hset_single_pair() {
        let cmd = is_ok(Command::parse(parts(&["HSET", "h", "f", "v"])));
        assert!(matches!(cmd, Command::HSet { pairs, .. } if pairs.len() == 1));
    }

    #[test]
    fn parse_hset_multiple_pairs() {
        let cmd = is_ok(Command::parse(parts(&["HSET", "h", "f1", "v1", "f2", "v2"])));
        assert!(matches!(cmd, Command::HSet { pairs, .. } if pairs.len() == 2));
    }

    #[test]
    fn parse_hset_missing_value() {
        is_err(Command::parse(parts(&["HSET", "h", "f"])));
    }

    #[test]
    fn parse_hdel() {
        let cmd = is_ok(Command::parse(parts(&["HDEL", "h", "f1", "f2"])));
        assert!(matches!(cmd, Command::HDel { fields, .. } if fields.len() == 2));
    }

    #[test]
    fn parse_hdel_missing_field() {
        is_err(Command::parse(parts(&["HDEL", "h"])));
    }

    #[test]
    fn parse_hgetall() {
        let cmd = is_ok(Command::parse(parts(&["HGETALL", "h"])));
        assert!(matches!(cmd, Command::HGetAll { key } if key == "h"));
    }

    #[test]
    fn parse_hmget() {
        let cmd = is_ok(Command::parse(parts(&["HMGET", "h", "f1", "f2", "f3"])));
        assert!(matches!(cmd, Command::HMGet { fields, .. } if fields.len() == 3));
    }

    #[test]
    fn parse_hmget_missing_field() {
        is_err(Command::parse(parts(&["HMGET", "h"])));
    }

    #[test]
    fn parse_hmset() {
        let cmd = is_ok(Command::parse(parts(&["HMSET", "h", "f1", "v1", "f2", "v2"])));
        assert!(matches!(cmd, Command::HMSet { pairs, .. } if pairs.len() == 2));
    }

    #[test]
    fn parse_hkeys() {
        let cmd = is_ok(Command::parse(parts(&["HKEYS", "h"])));
        assert!(matches!(cmd, Command::HKeys { key } if key == "h"));
    }

    #[test]
    fn parse_hvals() {
        let cmd = is_ok(Command::parse(parts(&["HVALS", "h"])));
        assert!(matches!(cmd, Command::HVals { key } if key == "h"));
    }

    #[test]
    fn parse_hexists() {
        let cmd = is_ok(Command::parse(parts(&["HEXISTS", "h", "field"])));
        assert!(matches!(cmd, Command::HExists { key, field } if key == "h" && field == "field"));
    }

    #[test]
    fn parse_hlen() {
        let cmd = is_ok(Command::parse(parts(&["HLEN", "h"])));
        assert!(matches!(cmd, Command::HLen { key } if key == "h"));
    }

    #[test]
    fn parse_hincrby() {
        let cmd = is_ok(Command::parse(parts(&["HINCRBY", "h", "f", "10"])));
        assert!(matches!(cmd, Command::HIncrBy { key, field, delta } if key == "h" && field == "f" && delta == 10));
    }

    #[test]
    fn parse_hincrby_invalid_delta() {
        is_err(Command::parse(parts(&["HINCRBY", "h", "f", "notanumber"])));
    }

    #[test]
    fn parse_hsetnx() {
        let cmd = is_ok(Command::parse(parts(&["HSETNX", "h", "f", "v"])));
        assert!(matches!(cmd, Command::HSetNx { key, field, value } if key == "h" && field == "f" && value == "v"));
    }

    // ───────────────────────────── Edge cases ────────────────────────────────

    #[test]
    fn parse_empty_parts() {
        is_err(Command::parse(vec![]));
    }

    #[test]
    fn parse_unknown_command() {
        is_err(Command::parse(parts(&["UNKNOWN"])));
    }

    #[test]
    fn parse_case_insensitive() {
        let cmd = is_ok(Command::parse(parts(&["get", "k"])));
        assert!(matches!(cmd, Command::Get { .. }));
        let cmd = is_ok(Command::parse(parts(&["Get", "k"])));
        assert!(matches!(cmd, Command::Get { .. }));
    }

    // ──────────────────────── glob_to_sql_like helper ────────────────────────

    #[test]
    fn glob_star_becomes_percent() {
        assert_eq!(glob_to_sql_like("*"), "%");
    }

    #[test]
    fn glob_question_becomes_underscore() {
        assert_eq!(glob_to_sql_like("?"), "_");
    }

    #[test]
    fn glob_prefix_pattern() {
        assert_eq!(glob_to_sql_like("user:*"), "user:%");
    }

    #[test]
    fn glob_escapes_sql_wildcards_in_pattern() {
        assert_eq!(glob_to_sql_like("a%b_c"), "a\\%b\\_c");
    }
}
