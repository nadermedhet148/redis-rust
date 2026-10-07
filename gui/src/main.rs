//! Desktop client for rkv. Talks the same line protocol as `nc`:
//! PING / GET <key> / SET <key> <value...> / DEL <key>.
//!
//! The server has no KEYS command, so the key table only shows keys this
//! window has touched, with the last value it saw for each.

mod client;

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Duration;

use eframe::egui::{self, Color32, Key, RichText};

use client::{Client, Event, Request};

const DEFAULT_ADDR: &str = "127.0.0.1:6380";
const LOG_CAP: usize = 1000;
const NIL: &str = "(nil)";

fn main() -> eframe::Result {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_title("rkv")
            .with_inner_size([1150.0, 720.0])
            .with_min_inner_size([820.0, 480.0]),
        ..Default::default()
    };
    eframe::run_native("rkv", options, Box::new(|cc| Ok(Box::new(App::new(cc)))))
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum ConnState {
    Disconnected,
    Connecting,
    Connected,
}

/// What a sent line means, so its reply can update the key table.
/// Mirrors `rkv::command::Command::parse` closely enough for that.
enum Op {
    Ping,
    Get(String),
    Set(String, String),
    Del(String),
    Other,
}

impl Op {
    fn classify(line: &str) -> Op {
        let line = line.trim();
        let (name, rest) = match line.split_once(char::is_whitespace) {
            Some((name, rest)) => (name, rest.trim_start()),
            None => (line, ""),
        };
        let single = || {
            let mut parts = rest.split_whitespace();
            match (parts.next(), parts.next()) {
                (Some(k), None) => Some(k.to_string()),
                _ => None,
            }
        };
        match name.to_ascii_uppercase().as_str() {
            "PING" if rest.is_empty() => Op::Ping,
            "GET" => single().map_or(Op::Other, Op::Get),
            "DEL" => single().map_or(Op::Other, Op::Del),
            "SET" => match rest.split_once(char::is_whitespace) {
                Some((k, v)) => Op::Set(k.to_string(), v.trim_start().to_string()),
                None => Op::Other,
            },
            _ => Op::Other,
        }
    }
}

struct LogEntry {
    request: String,
    reply: String,
    elapsed: Option<Duration>,
    error: bool,
}

enum KeyState {
    Value(String),
    Missing,
}

struct App {
    client: Client,
    conn: ConnState,
    addr: String,
    status: String,

    key: String,
    value: String,

    input: String,
    history: Vec<String>,
    history_pos: Option<usize>,
    log: VecDeque<LogEntry>,
    pending: HashMap<u64, (String, Op)>,
    next_id: u64,

    keys: BTreeMap<String, KeyState>,
    key_filter: String,
    last_ping: Option<Duration>,

    bench_n: usize,
    bench_value_len: usize,
    bench_running: bool,
    bench_result: Option<String>,
}

impl App {
    fn new(cc: &eframe::CreationContext<'_>) -> Self {
        let mut app = Self {
            client: Client::spawn(cc.egui_ctx.clone()),
            conn: ConnState::Disconnected,
            addr: DEFAULT_ADDR.into(),
            status: String::new(),
            key: String::new(),
            value: String::new(),
            input: String::new(),
            history: Vec::new(),
            history_pos: None,
            log: VecDeque::new(),
            pending: HashMap::new(),
            next_id: 1,
            keys: BTreeMap::new(),
            key_filter: String::new(),
            last_ping: None,
            bench_n: 10_000,
            bench_value_len: 16,
            bench_running: false,
            bench_result: None,
        };
        app.connect();
        app
    }

    fn connect(&mut self) {
        self.conn = ConnState::Connecting;
        self.status = format!("connecting to {}…", self.addr);
        self.client
            .request(Request::Connect(self.addr.trim().to_string()));
    }

    fn send(&mut self, line: String) {
        let line = line.trim().to_string();
        if line.is_empty() {
            return;
        }
        if line.contains(['\n', '\r']) {
            self.push_log(line, "client: one command per line".into(), None, true);
            return;
        }
        let id = self.next_id;
        self.next_id += 1;
        self.pending.insert(id, (line.clone(), Op::classify(&line)));
        self.client.request(Request::Send { id, line });
    }

    fn push_log(&mut self, request: String, reply: String, elapsed: Option<Duration>, error: bool) {
        if self.log.len() == LOG_CAP {
            self.log.pop_front();
        }
        self.log.push_back(LogEntry {
            request,
            reply,
            elapsed,
            error,
        });
    }

    fn drain_events(&mut self) {
        let events: Vec<Event> = self.client.poll().collect();
        for event in events {
            match event {
                Event::Connected(addr) => {
                    self.conn = ConnState::Connected;
                    self.status = format!("connected to {addr}");
                }
                Event::Disconnected { reason } => {
                    self.conn = ConnState::Disconnected;
                    self.bench_running = false;
                    self.status = reason.unwrap_or_else(|| "disconnected".into());
                }
                Event::Reply { id, reply, elapsed } => {
                    let Some((line, op)) = self.pending.remove(&id) else {
                        continue;
                    };
                    let error = reply.starts_with("ERR");
                    if !error {
                        self.apply(op, &reply, elapsed);
                    }
                    self.push_log(line, reply, Some(elapsed), error);
                }
                Event::Failed { id, error } => {
                    let line = self.pending.remove(&id).map(|(l, _)| l).unwrap_or_default();
                    self.conn = ConnState::Disconnected;
                    self.status = error.clone();
                    self.push_log(line, format!("client: {error}"), None, true);
                }
                Event::BenchDone { n, set, get } => {
                    self.bench_running = false;
                    self.bench_result = Some(format!(
                        "SET  {:>9.0} ops/s   avg {}\nGET  {:>9.0} ops/s   avg {}",
                        n as f64 / set.as_secs_f64(),
                        fmt_dur(set / n as u32),
                        n as f64 / get.as_secs_f64(),
                        fmt_dur(get / n as u32),
                    ));
                }
            }
        }
    }

    /// Fold a successful reply into the key table.
    fn apply(&mut self, op: Op, reply: &str, elapsed: Duration) {
        match op {
            Op::Ping => self.last_ping = Some(elapsed),
            Op::Get(k) => {
                let state = if reply == NIL {
                    KeyState::Missing
                } else {
                    KeyState::Value(reply.to_string())
                };
                self.keys.insert(k, state);
            }
            Op::Set(k, v) => {
                self.keys.insert(k, KeyState::Value(v));
            }
            Op::Del(k) => {
                self.keys.insert(k, KeyState::Missing);
            }
            Op::Other => {}
        }
    }

    fn connected(&self) -> bool {
        self.conn == ConnState::Connected
    }

    fn top_bar(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.heading(RichText::new("rkv").strong());
            ui.separator();
            ui.label("Server");
            let editable = self.conn == ConnState::Disconnected;
            let addr = ui.add_enabled(
                editable,
                egui::TextEdit::singleline(&mut self.addr).desired_width(160.0),
            );
            let enter = addr.lost_focus() && ui.input(|i| i.key_pressed(Key::Enter));
            match self.conn {
                ConnState::Connected => {
                    if ui.button("Disconnect").clicked() {
                        self.client.request(Request::Disconnect);
                    }
                }
                ConnState::Connecting => {
                    ui.add_enabled(false, egui::Button::new("Connecting…"));
                }
                ConnState::Disconnected => {
                    if ui.button("Connect").clicked() || enter {
                        self.connect();
                    }
                }
            }
            let color = match self.conn {
                ConnState::Connected => Color32::from_rgb(60, 180, 90),
                ConnState::Connecting => Color32::from_rgb(220, 170, 40),
                ConnState::Disconnected => Color32::from_rgb(210, 70, 70),
            };
            // Painted, not a glyph: the default font has no filled circle.
            let (rect, _) = ui.allocate_exact_size(egui::vec2(10.0, 10.0), egui::Sense::hover());
            ui.painter().circle_filled(rect.center(), 5.0, color);
            ui.label(&self.status);

            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui
                    .add_enabled(self.connected(), egui::Button::new("Ping"))
                    .clicked()
                {
                    self.send("PING".into());
                }
                if let Some(rtt) = self.last_ping {
                    ui.label(RichText::new(format!("RTT {}", fmt_dur(rtt))).monospace());
                }
            });
        });
    }

    fn ops_panel(&mut self, ui: &mut egui::Ui) {
        ui.heading("Operations");
        ui.add_space(4.0);
        egui::Grid::new("ops")
            .num_columns(2)
            .spacing([8.0, 6.0])
            .show(ui, |ui| {
                ui.label("Key");
                ui.add(
                    egui::TextEdit::singleline(&mut self.key)
                        .hint_text("user:42")
                        .desired_width(f32::INFINITY),
                );
                ui.end_row();
                ui.label("Value");
                ui.add(
                    egui::TextEdit::singleline(&mut self.value)
                        .hint_text("spaces are fine")
                        .desired_width(f32::INFINITY),
                );
                ui.end_row();
            });

        let key = self.key.trim().to_string();
        let key_ok = !key.is_empty() && !key.contains(char::is_whitespace);
        if !key.is_empty() && !key_ok {
            ui.colored_label(ui.visuals().warn_fg_color, "Keys can't contain spaces.");
        }
        let can = self.connected() && key_ok;
        ui.add_space(4.0);
        ui.horizontal(|ui| {
            if ui.add_enabled(can, egui::Button::new("GET")).clicked() {
                self.send(format!("GET {key}"));
            }
            let set_ok = can && !self.value.trim().is_empty();
            if ui
                .add_enabled(set_ok, egui::Button::new("SET"))
                .on_disabled_hover_text("SET needs a key and a non-empty value")
                .clicked()
            {
                self.send(format!("SET {key} {}", self.value));
            }
            if ui.add_enabled(can, egui::Button::new("DEL")).clicked() {
                self.send(format!("DEL {key}"));
            }
        });

        ui.add_space(12.0);
        ui.separator();
        ui.heading("Round-trip benchmark");
        ui.label(
            RichText::new(
                "Sequential SETs, then GETs, on bench:<i> keys. One request in flight, like nc.",
            )
            .small()
            .weak(),
        );
        ui.add_space(4.0);
        egui::Grid::new("bench")
            .num_columns(2)
            .spacing([8.0, 6.0])
            .show(ui, |ui| {
                ui.label("Requests");
                ui.add(
                    egui::DragValue::new(&mut self.bench_n)
                        .range(1..=1_000_000)
                        .speed(100),
                );
                ui.end_row();
                ui.label("Value bytes");
                ui.add(egui::DragValue::new(&mut self.bench_value_len).range(1..=65_536));
                ui.end_row();
            });
        ui.add_space(4.0);
        ui.horizontal(|ui| {
            let enabled = self.connected() && !self.bench_running;
            if ui.add_enabled(enabled, egui::Button::new("Run")).clicked() {
                self.bench_running = true;
                self.bench_result = None;
                self.client.request(Request::Bench {
                    n: self.bench_n,
                    value_len: self.bench_value_len,
                });
            }
            if self.bench_running {
                ui.spinner();
                ui.label("running…");
            }
        });
        if let Some(result) = &self.bench_result {
            ui.add_space(4.0);
            ui.label(RichText::new(result).monospace());
        }
    }

    fn keys_panel(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.heading("Keys");
            ui.label(RichText::new(format!("{}", self.keys.len())).weak());
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui
                    .small_button("Clear")
                    .on_hover_text("Forget all rows (doesn't touch the server)")
                    .clicked()
                {
                    self.keys.clear();
                }
                if ui
                    .add_enabled(
                        self.connected() && !self.keys.is_empty(),
                        egui::Button::new("Refresh").small(),
                    )
                    .on_hover_text("GET every key in the table")
                    .clicked()
                {
                    let keys: Vec<String> = self.keys.keys().cloned().collect();
                    for k in keys {
                        self.send(format!("GET {k}"));
                    }
                }
            });
        });
        ui.add(
            egui::TextEdit::singleline(&mut self.key_filter)
                .hint_text("filter…")
                .desired_width(f32::INFINITY),
        );
        ui.label(
            RichText::new("Only keys this window has touched: the server has no KEYS command.")
                .small()
                .weak(),
        );
        ui.add_space(4.0);

        let filter = self.key_filter.trim().to_lowercase();
        let mut send: Vec<String> = Vec::new();
        let mut forget: Option<String> = None;
        egui::ScrollArea::vertical()
            .auto_shrink([false, false])
            .show(ui, |ui| {
                egui::Grid::new("keys")
                    .num_columns(3)
                    .striped(true)
                    .spacing([10.0, 4.0])
                    .show(ui, |ui| {
                        for (k, state) in &self.keys {
                            if !filter.is_empty() && !k.to_lowercase().contains(&filter) {
                                continue;
                            }
                            if ui
                                .add(
                                    egui::Label::new(RichText::new(k).monospace().strong())
                                        .sense(egui::Sense::click()),
                                )
                                .on_hover_text("Click to load into the editor")
                                .clicked()
                            {
                                self.key = k.clone();
                                if let KeyState::Value(v) = state {
                                    self.value = v.clone();
                                }
                            }
                            match state {
                                KeyState::Value(v) => {
                                    ui.add(
                                        egui::Label::new(
                                            RichText::new(truncate(v, 48)).monospace(),
                                        )
                                        .truncate(),
                                    )
                                    .on_hover_text(v);
                                }
                                KeyState::Missing => {
                                    ui.label(RichText::new(NIL).monospace().weak().italics());
                                }
                            }
                            ui.horizontal(|ui| {
                                ui.add_enabled_ui(self.connected(), |ui| {
                                    if ui.small_button("get").clicked() {
                                        send.push(format!("GET {k}"));
                                    }
                                    if ui.small_button("del").clicked() {
                                        send.push(format!("DEL {k}"));
                                    }
                                });
                                if ui
                                    .small_button("✕")
                                    .on_hover_text("Forget this row")
                                    .clicked()
                                {
                                    forget = Some(k.clone());
                                }
                            });
                            ui.end_row();
                        }
                    });
            });
        for line in send {
            self.send(line);
        }
        if let Some(k) = forget {
            self.keys.remove(&k);
        }
    }

    fn console(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.heading("Console");
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui.small_button("Clear").clicked() {
                    self.log.clear();
                }
            });
        });

        // Input pinned to the bottom, log fills the rest.
        egui::Panel::bottom("console_input")
            .frame(egui::Frame::NONE)
            .show(ui, |ui| {
                ui.add_space(6.0);
                ui.horizontal(|ui| {
                    ui.label(RichText::new(">").monospace().strong());
                    let response = ui.add(
                        egui::TextEdit::singleline(&mut self.input)
                            .font(egui::TextStyle::Monospace)
                            .hint_text("SET greeting hello world   (Up/Down for history)")
                            .desired_width(f32::INFINITY),
                    );
                    if response.has_focus() {
                        self.history_keys(ui);
                    }
                    if response.lost_focus() && ui.input(|i| i.key_pressed(Key::Enter)) {
                        let line = std::mem::take(&mut self.input);
                        if !line.trim().is_empty() {
                            if self.history.last() != Some(&line) {
                                self.history.push(line.clone());
                            }
                            self.history_pos = None;
                            self.send(line);
                        }
                        response.request_focus();
                    }
                });
            });

        egui::ScrollArea::vertical()
            .auto_shrink([false, false])
            .stick_to_bottom(true)
            .show(ui, |ui| {
                if self.log.is_empty() {
                    ui.label(
                        RichText::new(
                            "Commands: PING · GET <key> · SET <key> <value…> · DEL <key>",
                        )
                        .weak(),
                    );
                }
                let err = ui.visuals().error_fg_color;
                for entry in &self.log {
                    ui.horizontal_wrapped(|ui| {
                        ui.label(
                            RichText::new(format!("> {}", entry.request))
                                .monospace()
                                .strong(),
                        );
                        if let Some(d) = entry.elapsed {
                            ui.label(RichText::new(fmt_dur(d)).monospace().small().weak());
                        }
                    });
                    let reply = RichText::new(&entry.reply).monospace();
                    ui.label(if entry.error { reply.color(err) } else { reply });
                    ui.add_space(2.0);
                }
            });
    }

    fn history_keys(&mut self, ui: &egui::Ui) {
        if self.history.is_empty() {
            return;
        }
        let (up, down) = ui.input(|i| (i.key_pressed(Key::ArrowUp), i.key_pressed(Key::ArrowDown)));
        if up {
            let pos = match self.history_pos {
                None => self.history.len() - 1,
                Some(p) => p.saturating_sub(1),
            };
            self.history_pos = Some(pos);
            self.input = self.history[pos].clone();
        } else if down {
            match self.history_pos {
                Some(p) if p + 1 < self.history.len() => {
                    self.history_pos = Some(p + 1);
                    self.input = self.history[p + 1].clone();
                }
                Some(_) => {
                    self.history_pos = None;
                    self.input.clear();
                }
                None => {}
            }
        }
    }
}

impl eframe::App for App {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.drain_events();

        let top_frame =
            egui::Frame::side_top_panel(ui.style()).inner_margin(egui::Margin::symmetric(12, 8));
        egui::Panel::top("top")
            .frame(top_frame)
            .show(ui, |ui| self.top_bar(ui));
        egui::Panel::left("ops")
            .resizable(true)
            .default_size(300.0)
            .show(ui, |ui| {
                ui.add_space(8.0);
                self.ops_panel(ui);
            });
        egui::Panel::right("keys")
            .resizable(true)
            .default_size(380.0)
            .show(ui, |ui| {
                ui.add_space(8.0);
                self.keys_panel(ui);
            });
        egui::CentralPanel::default().show(ui, |ui| self.console(ui));
    }
}

fn fmt_dur(d: Duration) -> String {
    let us = d.as_secs_f64() * 1e6;
    if us < 1000.0 {
        format!("{us:.0} µs")
    } else if us < 1e6 {
        format!("{:.2} ms", us / 1e3)
    } else {
        format!("{:.2} s", us / 1e6)
    }
}

fn truncate(s: &str, max: usize) -> String {
    match s.char_indices().nth(max) {
        Some((i, _)) => format!("{}…", &s[..i]),
        None => s.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_matches_server_grammar() {
        assert!(matches!(Op::classify("ping"), Op::Ping));
        assert!(matches!(Op::classify("PING x"), Op::Other));
        assert!(matches!(Op::classify("get  foo "), Op::Get(k) if k == "foo"));
        assert!(matches!(Op::classify("GET a b"), Op::Other));
        assert!(
            matches!(Op::classify("SET k hello  world"), Op::Set(k, v) if k == "k" && v == "hello  world")
        );
        assert!(matches!(Op::classify("SET k"), Op::Other));
        assert!(matches!(Op::classify("dEl foo"), Op::Del(k) if k == "foo"));
    }

    #[test]
    fn truncate_is_char_safe() {
        assert_eq!(truncate("héllo", 3), "hél…");
        assert_eq!(truncate("hi", 3), "hi");
    }
}
