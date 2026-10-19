//! Prepared plans for the table backend, kept across transactions.
//!
//! `SpiClient::select`/`update` with SQL text go through
//! `SPI_execute_with_args`, which parses and plans the statement on every call.
//! For the short primary-key statements most commands are, that planning costs
//! more than running them. Here each statement is prepared once per worker and
//! kept (`SPI_keepplan`), keyed by its text and its arguments' types; later
//! calls only execute it. PostgreSQL's plan cache replans a kept plan itself
//! when a table it reads changes shape.
//!
//! The text of a few statements carries a value of its own (a `LIMIT n`), so
//! the cache is bounded: past `CAPACITY` it drops the plan used least recently.
#![forbid(unsafe_code)]

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use pgrx::datum::DatumWithOid;
use pgrx::pg_sys;
use pgrx::spi::{OwnedPreparedStatement, SpiClient, SpiResult, SpiTupleTable};

const CAPACITY: usize = 512;

struct Entry {
    plan: Rc<OwnedPreparedStatement>,
    used: u64,
}

#[derive(Default)]
struct Cache {
    plans: HashMap<(String, Vec<pg_sys::Oid>), Entry>,
    clock: u64,
}

thread_local! {
    static CACHE: RefCell<Cache> = RefCell::new(Cache::default());
}

/// The kept plan for `sql`, prepared on first use. `mutating` is what
/// `update` rather than `select` would have said.
fn plan(
    client: &SpiClient<'_>,
    sql: &str,
    args: &[DatumWithOid],
    mutating: bool,
) -> SpiResult<Rc<OwnedPreparedStatement>> {
    let key = (
        sql.to_string(),
        args.iter().map(DatumWithOid::oid).collect::<Vec<_>>(),
    );
    // Taken out of the cache before it runs: running it may come back here
    // (a trigger on a routed table, say), and the cache must not be borrowed.
    CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        cache.clock += 1;
        let now = cache.clock;
        if let Some(entry) = cache.plans.get_mut(&key) {
            entry.used = now;
            return Ok(Rc::clone(&entry.plan));
        }
        let oids: Vec<pgrx::PgOid> = key.1.iter().map(|o| pgrx::PgOid::from(*o)).collect();
        let prepared = if mutating {
            client.prepare_mut(sql, &oids)?
        } else {
            client.prepare(sql, &oids)?
        };
        let plan = Rc::new(prepared.keep());
        if cache.plans.len() >= CAPACITY
            && let Some(oldest) = cache
                .plans
                .iter()
                .min_by_key(|(_, e)| e.used)
                .map(|(k, _)| k.clone())
        {
            cache.plans.remove(&oldest);
        }
        cache.plans.insert(
            key,
            Entry {
                plan: Rc::clone(&plan),
                used: now,
            },
        );
        Ok(plan)
    })
}

/// `SpiClient::select`, from a kept plan.
pub fn select<'conn>(
    client: &SpiClient<'conn>,
    sql: &str,
    args: &[DatumWithOid],
) -> SpiResult<SpiTupleTable<'conn>> {
    let plan = plan(client, sql, args, false)?;
    client.select(&*plan, None, args)
}

/// `SpiClient::update`, from a kept plan.
pub fn update<'conn>(
    client: &mut SpiClient<'conn>,
    sql: &str,
    args: &[DatumWithOid],
) -> SpiResult<SpiTupleTable<'conn>> {
    let plan = plan(client, sql, args, true)?;
    client.update(&*plan, None, args)
}

/// `select`/`update` with a kept plan, for call sites that read like the
/// `SpiClient` methods they replace.
pub trait Kept<'conn> {
    fn select_kept(&self, sql: &str, args: &[DatumWithOid]) -> SpiResult<SpiTupleTable<'conn>>;
    fn update_kept(&mut self, sql: &str, args: &[DatumWithOid]) -> SpiResult<SpiTupleTable<'conn>>;
}

impl<'conn> Kept<'conn> for SpiClient<'conn> {
    fn select_kept(&self, sql: &str, args: &[DatumWithOid]) -> SpiResult<SpiTupleTable<'conn>> {
        select(self, sql, args)
    }

    fn update_kept(&mut self, sql: &str, args: &[DatumWithOid]) -> SpiResult<SpiTupleTable<'conn>> {
        update(self, sql, args)
    }
}
