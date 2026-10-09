//! Leader → replica replication, in-process: every node is a real TCP server
//! on 127.0.0.1, talking the real protocol.
//!
//! Network failures are simulated with a small TCP proxy between a replica and
//! its leader: it can cut every connection, refuse new ones for a while, or
//! start forwarding to a different leader (which looks like a leader restart).

use std::future::Future;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use rkv::db::{Db, StoreKind};
use rkv::repl::Node;

const KINDS: [StoreKind; 3] = [StoreKind::Mutex, StoreKind::Rwlock, StoreKind::Sharded];

struct TestNode {
    addr: SocketAddr,
    server: JoinHandle<anyhow::Result<()>>,
}

impl TestNode {
    async fn start(node: Arc<Node>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(rkv::server::serve(listener, node));
        Self { addr, server }
    }

    async fn leader(kind: StoreKind) -> Self {
        Self::start(Node::new(Db::new(kind))).await
    }

    async fn replica_of(kind: StoreKind, leader: SocketAddr) -> Self {
        let node = Node::new(Db::new(kind));
        node.replicate_from(leader.to_string());
        Self::start(node).await
    }

    async fn client(&self) -> Client {
        Client::connect(self.addr).await
    }

    async fn cmd(&self, line: &str) -> String {
        self.client().await.send(line).await
    }

    async fn role_field(&self, name: &str) -> String {
        field(&self.cmd("ROLE").await, name)
    }

    /// `host port`, as REPLICAOF wants it.
    fn host_port(&self) -> String {
        format!("{} {}", self.addr.ip(), self.addr.port())
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        self.server.abort();
    }
}

struct Client {
    lines: tokio::io::Lines<BufReader<tokio::net::tcp::OwnedReadHalf>>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl Client {
    async fn connect(addr: SocketAddr) -> Self {
        let (r, w) = TcpStream::connect(addr).await.unwrap().into_split();
        Self {
            lines: BufReader::new(r).lines(),
            writer: w,
        }
    }

    async fn send(&mut self, cmd: &str) -> String {
        self.writer
            .write_all(format!("{cmd}\n").as_bytes())
            .await
            .unwrap();
        self.lines.next_line().await.unwrap().unwrap()
    }
}

/// `key=value` field from a ROLE reply.
fn field(role: &str, name: &str) -> String {
    role.split_whitespace()
        .find_map(|kv| kv.strip_prefix(&format!("{name}=")))
        .unwrap_or_else(|| panic!("no {name}= in {role:?}"))
        .to_string()
}

/// Poll `check` until it returns true; panic after 10 s.
async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !check().await {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_in_sync(leader: &TestNode, replica: &TestNode) {
    eventually("replica digest == leader digest", || async {
        leader.cmd("DIGEST").await == replica.cmd("DIGEST").await
    })
    .await;
}

/// Forwards TCP connections to `target`. Can cut them, refuse new ones, or
/// switch to another target.
struct Proxy {
    addr: SocketAddr,
    target: Arc<Mutex<SocketAddr>>,
    blocked: Arc<AtomicBool>,
    links: Arc<Mutex<Vec<JoinHandle<()>>>>,
    accept: JoinHandle<()>,
}

impl Proxy {
    async fn start(target: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let target = Arc::new(Mutex::new(target));
        let blocked = Arc::new(AtomicBool::new(false));
        let links: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::default();
        let accept = tokio::spawn({
            let (target, blocked, links) = (target.clone(), blocked.clone(), links.clone());
            async move {
                loop {
                    let (mut client, _) = listener.accept().await.unwrap();
                    if blocked.load(Ordering::SeqCst) {
                        continue; // dropped: connection refused, in effect
                    }
                    let to = *target.lock().unwrap();
                    links.lock().unwrap().push(tokio::spawn(async move {
                        if let Ok(mut server) = TcpStream::connect(to).await {
                            let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
                        }
                    }));
                }
            }
        });
        Self {
            addr,
            target,
            blocked,
            links,
            accept,
        }
    }

    /// Cut every open connection and refuse new ones until `unblock`.
    fn cut_and_block(&self) {
        self.blocked.store(true, Ordering::SeqCst);
        for link in self.links.lock().unwrap().drain(..) {
            link.abort();
        }
    }

    fn unblock(&self) {
        self.blocked.store(false, Ordering::SeqCst);
    }

    fn retarget(&self, target: SocketAddr) {
        *self.target.lock().unwrap() = target;
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.accept.abort();
        self.cut_and_block();
    }
}

#[tokio::test]
async fn replica_follows_sets_and_deletes() {
    let leader = TestNode::leader(StoreKind::Sharded).await;
    let replica = TestNode::replica_of(StoreKind::Sharded, leader.addr).await;
    let mut c = leader.client().await;
    for i in 0..200 {
        assert_eq!(c.send(&format!("SET k{i} value {i}")).await, "OK");
    }
    for i in (0..200).step_by(4) {
        assert_eq!(c.send(&format!("DEL k{i}")).await, "1");
    }
    wait_in_sync(&leader, &replica).await;
    assert_eq!(replica.cmd("DBSIZE").await, "150");
    assert_eq!(replica.cmd("GET k1").await, "value 1");
    assert_eq!(replica.cmd("GET k0").await, "(nil)");
}

#[tokio::test]
async fn replica_rejects_client_writes() {
    let leader = TestNode::leader(StoreKind::Mutex).await;
    let replica = TestNode::replica_of(StoreKind::Mutex, leader.addr).await;
    for cmd in ["SET k v", "DEL k"] {
        assert!(replica.cmd(cmd).await.starts_with("ERR READONLY"), "{cmd}");
    }
    assert_eq!(replica.cmd("GET k").await, "(nil)");
}

#[tokio::test]
async fn full_sync_copies_data_written_before_the_replica_existed() {
    let leader = TestNode::leader(StoreKind::Sharded).await;
    let mut c = leader.client().await;
    for i in 0..1000 {
        c.send(&format!("SET k{i} {i}")).await;
    }
    let replica = TestNode::replica_of(StoreKind::Mutex, leader.addr).await;
    wait_in_sync(&leader, &replica).await;
    assert_eq!(replica.cmd("DBSIZE").await, "1000");
    assert_eq!(leader.role_field("full_syncs").await, "1");
    eventually("replica reports streaming", || async {
        replica.role_field("state").await == "streaming"
    })
    .await;
}

/// The fuzzy-snapshot argument, tested: the replica attaches while 20 clients
/// are writing and deleting, so the snapshot is copied shard by shard while
/// keys change underneath. After the load stops, the replica must be identical.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replica_attached_under_load_converges_on_every_store() {
    for kind in KINDS {
        let leader = TestNode::leader(kind).await;
        let writers: Vec<_> = (0..20)
            .map(|w| {
                let addr = leader.addr;
                tokio::spawn(async move {
                    let mut c = Client::connect(addr).await;
                    for i in 0..300u32 {
                        let key = format!("k{}", (w * 7 + i) % 100);
                        if i % 5 == 0 {
                            c.send(&format!("DEL {key}")).await;
                        } else {
                            c.send(&format!("SET {key} w{w}-{i}")).await;
                        }
                    }
                })
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let replica = TestNode::replica_of(StoreKind::Sharded, leader.addr).await;
        for w in writers {
            w.await.unwrap();
        }
        wait_in_sync(&leader, &replica).await;
        assert_eq!(leader.cmd("DBSIZE").await, replica.cmd("DBSIZE").await);
    }
}

#[tokio::test]
async fn network_blip_heals_with_partial_resync() {
    let leader = TestNode::leader(StoreKind::Sharded).await;
    let proxy = Proxy::start(leader.addr).await;
    let replica = TestNode::replica_of(StoreKind::Sharded, proxy.addr).await;
    leader.cmd("SET before blip").await;
    wait_in_sync(&leader, &replica).await;

    proxy.cut_and_block();
    for i in 0..50 {
        leader.cmd(&format!("SET during{i} blip")).await;
    }
    leader.cmd("DEL before").await;
    proxy.unblock();

    wait_in_sync(&leader, &replica).await;
    assert_eq!(replica.cmd("GET during49").await, "blip");
    assert_eq!(replica.cmd("GET before").await, "(nil)");
    assert_eq!(leader.role_field("full_syncs").await, "1");
    assert_eq!(leader.role_field("partial_syncs").await, "1");
}

#[tokio::test]
async fn falling_out_of_the_backlog_forces_a_full_sync() {
    let leader = TestNode::start(Node::with_backlog(Db::new(StoreKind::Sharded), 4096)).await;
    let proxy = Proxy::start(leader.addr).await;
    let replica = TestNode::replica_of(StoreKind::Sharded, proxy.addr).await;
    leader.cmd("SET k v").await;
    wait_in_sync(&leader, &replica).await;

    proxy.cut_and_block();
    let mut c = leader.client().await;
    let big = "x".repeat(200);
    for i in 0..100 {
        c.send(&format!("SET big{i} {big}")).await; // ~20 KB > 4 KB backlog
    }
    proxy.unblock();

    wait_in_sync(&leader, &replica).await;
    assert_eq!(replica.cmd("DBSIZE").await, "101");
    assert_eq!(leader.role_field("full_syncs").await, "2");
    assert_eq!(leader.role_field("partial_syncs").await, "0");
}

/// A restarted leader has a new replid, and with `--fsync every-sec` it may
/// have lost writes the replica already applied. The replica must not
/// "continue" into a different history: it full syncs and drops what the new
/// leader doesn't have.
#[tokio::test]
async fn new_leader_replid_forces_full_sync() {
    let old = TestNode::leader(StoreKind::Sharded).await;
    let proxy = Proxy::start(old.addr).await;
    let replica = TestNode::replica_of(StoreKind::Sharded, proxy.addr).await;
    old.cmd("SET only-on-old 1").await;
    old.cmd("SET shared old").await;
    wait_in_sync(&old, &replica).await;

    let new = TestNode::leader(StoreKind::Sharded).await;
    new.cmd("SET shared new").await;
    proxy.cut_and_block();
    proxy.retarget(new.addr);
    proxy.unblock();

    wait_in_sync(&new, &replica).await;
    assert_eq!(replica.cmd("GET only-on-old").await, "(nil)");
    assert_eq!(replica.cmd("GET shared").await, "new");
    assert_eq!(
        replica.role_field("leader_replid").await,
        new.role_field("replid").await
    );
}

/// Manual failover: promote one replica, point the other one at it.
#[tokio::test]
async fn promote_a_replica_and_repoint_the_other() {
    let leader = TestNode::leader(StoreKind::Sharded).await;
    let r1 = TestNode::replica_of(StoreKind::Sharded, leader.addr).await;
    let r2 = TestNode::replica_of(StoreKind::Sharded, leader.addr).await;
    for i in 0..100 {
        leader.cmd(&format!("SET k{i} {i}")).await;
    }
    wait_in_sync(&leader, &r1).await;
    wait_in_sync(&leader, &r2).await;

    assert_eq!(r1.cmd("REPLICAOF NO ONE").await, "OK");
    assert!(r1.cmd("ROLE").await.starts_with("leader "));
    assert_eq!(r1.cmd("SET after failover").await, "OK");
    assert_eq!(r2.cmd(&format!("REPLICAOF {}", r1.host_port())).await, "OK");

    wait_in_sync(&r1, &r2).await;
    assert_eq!(r2.cmd("GET after").await, "failover");
    assert_eq!(r2.cmd("DBSIZE").await, "101");
    assert_eq!(r1.role_field("replicas").await, "1");
}

#[tokio::test]
async fn leader_reports_replica_lag_from_acks() {
    let leader = TestNode::leader(StoreKind::Sharded).await;
    let _replica = TestNode::replica_of(StoreKind::Sharded, leader.addr).await;
    for i in 0..10 {
        leader.cmd(&format!("SET k{i} {i}")).await;
    }
    eventually("an ACK covering every write", || async {
        let role = leader.cmd("ROLE").await;
        role.contains("replicas=1") && field(&role, "offset") != "0" && role.contains("lag=0")
    })
    .await;
}

/// A replica can itself have replicas: L → R1 → R2.
#[tokio::test]
async fn chained_replication() {
    let leader = TestNode::leader(StoreKind::Sharded).await;
    let r1 = TestNode::replica_of(StoreKind::Sharded, leader.addr).await;
    let r2 = TestNode::replica_of(StoreKind::Mutex, r1.addr).await;
    for i in 0..100 {
        leader.cmd(&format!("SET k{i} {i}")).await;
    }
    leader.cmd("DEL k5").await;
    wait_in_sync(&leader, &r2).await;
    assert_eq!(r2.cmd("DBSIZE").await, "99");
}
