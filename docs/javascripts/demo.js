// The live demo on the home page: PostgreSQL with pg_redis, booted in this tab.
//
// The machine is a snowglobe build of docker/demo (`mise run demo` writes it to docs/demo/):
// an arm64 Linux snapshot restored by armless, a CPU emulator compiled to WebAssembly. This
// file uses only the build's assets (snapshot, filesystem blobs, armless and xterm) and boots
// the machine straight into the page, so the terminal is ours to style and to type into.
//
// Inside, tmux holds redis-cli (top pane) and psql (bottom); alt-1 and alt-2 switch between them.
(() => {
  const root = document.querySelector("[data-demo]");
  if (!root) return;

  const base = new URL(root.dataset.demo, document.baseURI);
  const at = (path) => new URL(path, base).href;
  const stage = root.querySelector(".pr-demo__stage");
  const screen = root.querySelector(".pr-demo__screen");
  const boot = root.querySelector(".pr-demo__boot");
  const status = root.querySelector(".pr-demo__status");
  const steps = [...root.querySelectorAll(".pr-step")];
  const PANE = { redis: "\x1b1", sql: "\x1b2" };

  let emulator = null;
  let started = false;

  const setStatus = (state, text) => {
    root.dataset.state = state;
    status.textContent = text;
  };

  const script = (src) =>
    new Promise((resolve, reject) => {
      const el = document.createElement("script");
      el.src = src;
      el.onload = resolve;
      el.onerror = () => reject(new Error(`could not load ${src}`));
      document.head.append(el);
    });

  const stylesheet = (href) => {
    const el = document.createElement("link");
    el.rel = "stylesheet";
    el.href = href;
    document.head.append(el);
  };

  async function start() {
    if (started) return;
    started = true;
    setStatus("loading", "loading the machine");

    try {
      const run = await fetch(at("run.json")).then((r) => {
        if (!r.ok) throw new Error("demo not built");
        return r.json();
      });
      stylesheet(at("xterm/xterm.css"));
      await Promise.all([script(at("xterm/xterm.js")), script(at("armless/armless.js"))]);
      await script(at("xterm/addon-fit.js"));

      // Everything here must match the snapshot: same RAM and CPU count. One CPU, so no
      // cross-origin isolation is needed and the docs page keeps its ordinary headers.
      emulator = new Armless({
        wasm: at("armless/armless.wasm"),
        wasm_smp: at("armless/armless-smp.wasm"),
        memory_mb: run.memory_mb,
        cpus: run.cpus,
        filesystem: { json: at("system/filesystem.json"), baseurl: at("system/filesystem/") },
        initial_state: at("system/state.bin.zst"),
        autostart: false,
      });

      emulator.on("progress", (e) => {
        if (e.file.endsWith("state.bin.zst") && e.total) {
          setStatus("loading", `restoring ${Math.round((e.loaded / e.total) * 100)}%`);
        }
      });
      emulator.on("error", (error) => setStatus("error", `failed: ${error.message || error}`));

      const replay = fetch(at("system/console.bin"))
        .then((r) => (r.ok ? r.arrayBuffer() : null))
        .catch(() => null);

      await emulator.ready;

      const term = new Terminal({
        cursorBlink: true,
        scrollback: 2000,
        fontFamily: '"JetBrains Mono", ui-monospace, Menlo, monospace',
        fontSize: matchMedia("(max-width: 40em)").matches ? 11 : 13,
        theme: {
          background: "#101010",
          foreground: "#e6e6e6",
          cursor: "#3ecf8e",
          selectionBackground: "#3ecf8e55",
          green: "#3ecf8e",
          brightGreen: "#4ae29e",
        },
      });
      const fit = new FitAddon.FitAddon();
      term.loadAddon(fit);
      screen.replaceChildren();
      term.open(screen);
      term.textarea?.setAttribute("autocapitalize", "off");
      term.textarea?.setAttribute("spellcheck", "false");

      // Draw what tmux last drew at the snapshot's size, then fit and let tmux redraw
      term.resize(run.console_cols || 90, run.console_rows || 30);
      const bytes = await replay;
      if (bytes) await new Promise((resolve) => term.write(new Uint8Array(bytes), resolve));

      emulator.on("console", (out) => term.write(out));
      term.onData((data) => emulator.console_input(data));

      const resize = () => {
        fit.fit();
        emulator.console_resize(term.cols, term.rows);
      };
      new ResizeObserver(resize).observe(screen);
      resize();

      emulator.run();
      root.classList.add("is-running");
      setStatus("ready", "running in this tab");
      term.focus();
      preloadWarmPack().catch(() => {});

      root._term = term;
    } catch (error) {
      started = false;
      setStatus("error", error.message === "demo not built" ? "demo not built (mise run demo)" : `failed: ${error.message}`);
    }
  }

  // The files the first commands read, in one background download instead of one request each
  async function preloadWarmPack() {
    const response = await fetch(at("system/warm.pack"));
    if (!response.ok) return;
    const pack = new Uint8Array(await response.arrayBuffer());
    const indexSize = new DataView(pack.buffer).getUint32(0, true);
    const index = JSON.parse(new TextDecoder().decode(pack.subarray(4, 4 + indexSize)));
    let offset = 4 + indexSize;
    for (const [name, , length] of index) {
      const blob = pack.subarray(offset, offset + length);
      offset += length;
      if (!emulator.has_blob(name)) emulator.provide_blob(name, blob);
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
  }

  function send(step) {
    if (!emulator || root.dataset.state !== "ready") return;
    const pane = PANE[step.dataset.pane];
    // Switch pane, clear whatever is half-typed there (ctrl-u), then type the command
    emulator.console_input(`${pane}\x15${step.dataset.cmd}\r`);
    step.classList.add("is-done");
    const next = steps[steps.indexOf(step) + 1];
    steps.forEach((s) => s.classList.toggle("is-next", s === next));
    // Stacked on a phone, the terminal is above the steps: bring it back into
    // view so the step's output is seen
    if (matchMedia("(max-width: 52em)").matches) {
      const smooth = !matchMedia("(prefers-reduced-motion: reduce)").matches;
      stage.scrollIntoView({ block: "start", behavior: smooth ? "smooth" : "auto" });
    }
    root._term?.focus();
  }

  boot.addEventListener("click", start);
  for (const step of steps) {
    step.addEventListener("click", async () => {
      if (!started) await start();
      send(step);
    });
  }
  stage.addEventListener("keydown", (e) => {
    if (!started && (e.key === "Enter" || e.key === " ") && e.target === stage) {
      e.preventDefault();
      start();
    }
  });
})();
