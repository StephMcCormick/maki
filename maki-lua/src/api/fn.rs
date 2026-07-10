use std::collections::HashMap;
use std::env;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use mlua::{Function, Lua, RegistryKey, Result as LuaResult, Table};

use crate::api::fs::expand_tilde;
use crate::runtime::with_task_jobs;

use crate::plugin_permissions::{
    Permission::{Env, Run},
    PluginPermissions,
};

const READER_BUF_SIZE: usize = 8 * 1024;

#[derive(Clone)]
pub(crate) enum JobEvent {
    Stdout(String),
    Stderr(String),
    StdoutChunk(String),
    StderrChunk(String),
    Exit(i32),
}

enum JobKind {
    Process { pid: u32 },
}

struct JobMeta {
    kind: JobKind,
    alive: bool,
    on_stdout: Option<RegistryKey>,
    on_stderr: Option<RegistryKey>,
    on_stdout_chunk: Option<RegistryKey>,
    on_stderr_chunk: Option<RegistryKey>,
    on_exit: Option<RegistryKey>,
    event_rx: Option<flume::Receiver<JobEvent>>,
    stdin_tx: Option<flume::Sender<String>>,
}

pub(crate) struct JobStore {
    jobs: HashMap<u32, JobMeta>,
    next_id: u32,
}

impl JobStore {
    pub fn new() -> Self {
        Self {
            jobs: HashMap::new(),
            next_id: 1,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn start(
        &mut self,
        cmd: &str,
        cwd: Option<String>,
        env: Option<HashMap<String, String>>,
        use_stdin: bool,
        on_stdout: Option<RegistryKey>,
        on_stderr: Option<RegistryKey>,
        on_stdout_chunk: Option<RegistryKey>,
        on_stderr_chunk: Option<RegistryKey>,
        on_exit: Option<RegistryKey>,
    ) -> Result<u32, String> {
        let mut command = shell_command(cmd);
        command.stdout(Stdio::piped()).stderr(Stdio::piped());

        if use_stdin {
            command.stdin(Stdio::piped());
        } else {
            command.stdin(Stdio::null());
        }

        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            unsafe {
                command.pre_exec(|| {
                    libc::setsid();
                    Ok(())
                });
            }
        }

        if let Some(dir) = cwd.as_deref().map(expand_tilde) {
            if !dir.is_dir() {
                return Err(format!("cwd is not a directory: {}", dir.display()));
            }
            command.current_dir(dir);
        }
        if let Some(ref env_map) = env {
            for (k, v) in env_map {
                command.env(k, v);
            }
        }

        let mut child = command.spawn().map_err(|e| e.to_string())?;
        let pid = child.id();
        let id = self.next_id;
        self.next_id += 1;

        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let (event_tx, event_rx) = flume::unbounded();

        // Single reader per stream: reads raw chunks, delivers both line and chunk events.
        // This avoids competing readers on the same pipe.
        macro_rules! spawn_combined_reader {
            ($stream:expr, $name:expr, $line_variant:ident, $chunk_variant:ident) => {
                if let Some(mut stream) = $stream {
                    let tx = event_tx.clone();
                    Some(
                        thread::Builder::new()
                            .name($name.into())
                            .spawn(move || {
                                let mut buf = [0u8; READER_BUF_SIZE];
                                let mut partial = String::new();
                                loop {
                                    let n = std::io::Read::read(&mut stream, &mut buf).unwrap_or(0);
                                    if n == 0 {
                                        break;
                                    }
                                    let chunk = buf[..n].to_vec();

                                    // Deliver chunk event if callback exists
                                    if let Ok(chunk_str) = String::from_utf8(chunk.clone()) {
                                        if tx.send(JobEvent::$chunk_variant(chunk_str.clone())).is_err() {
                                            break;
                                        }

                                        // Deliver line events: split chunk by \n
                                        // \r may be present (preserved by BufReader::lines semantics)
                                        let mut pos = 0;
                                        for (i, b) in chunk_str.bytes().enumerate() {
                                            if b == b'\n' {
                                                let line = &chunk_str[pos..i];
                                                // Strip trailing \r (matching BufReader::lines behavior)
                                                let line = line.strip_suffix('\r').unwrap_or(line).to_string();
                                                if tx.send(JobEvent::$line_variant(line)).is_err() {
                                                    return;
                                                }
                                                pos = i + 1;
                                            }
                                        }
                                        // Accumulate partial line for next chunk
                                        partial = chunk_str[pos..].to_string();
                                    }
                                }

                                // Flush remaining partial line
                                if !partial.is_empty() {
                                    let line = partial.strip_suffix('\r').unwrap_or(&partial).to_string();
                                    let _ = tx.send(JobEvent::$line_variant(line));
                                }
                            })
                            .map_err(|e| e.to_string())?,
                    )
                } else {
                    None
                }
            };
        }
        let stdout_handle = spawn_combined_reader!(stdout, "job-stdout", Stdout, StdoutChunk);
        let stderr_handle = spawn_combined_reader!(stderr, "job-stderr", Stderr, StderrChunk);

        let stdin_tx = if use_stdin {
            let (tx, rx) = flume::bounded::<String>(64);
            let mut stdin = child.stdin.take().expect("stdin should be piped");
            thread::Builder::new()
                .name("job-stdin".into())
                .spawn(move || {
                    for data in rx.iter() {
                        if stdin.write_all(data.as_bytes()).is_err() {
                            break;
                        }
                    }
                    drop(stdin);
                })
                .map_err(|e| e.to_string())?;
            Some(tx)
        } else {
            None
        };

        thread::Builder::new()
            .name("job-wait".into())
            .spawn(move || {
                let code = child.wait().map(|s| s.code().unwrap_or(-1)).unwrap_or(-1);
                if let Some(h) = stdout_handle {
                    let _ = h.join();
                }
                if let Some(h) = stderr_handle {
                    let _ = h.join();
                }
                let _ = event_tx.send(JobEvent::Exit(code));
            })
            .map_err(|e| e.to_string())?;

        self.jobs.insert(
            id,
            JobMeta {
                kind: JobKind::Process { pid },
                alive: true,
                on_stdout,
                on_stderr,
                on_stdout_chunk,
                on_stderr_chunk,
                on_exit,
                event_rx: Some(event_rx),
                stdin_tx,
            },
        );

        Ok(id)
    }

    pub fn has_alive_jobs(&self) -> bool {
        self.jobs.values().any(|j| j.alive)
    }

    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }

    pub fn callback_key(&self, job_id: u32, event: &JobEvent) -> Option<&RegistryKey> {
        let meta = self.jobs.get(&job_id)?;
        match event {
            JobEvent::Stdout(_) => meta.on_stdout.as_ref(),
            JobEvent::Stderr(_) => meta.on_stderr.as_ref(),
            JobEvent::StdoutChunk(_) => meta.on_stdout_chunk.as_ref(),
            JobEvent::StderrChunk(_) => meta.on_stderr_chunk.as_ref(),
            JobEvent::Exit(_) => meta.on_exit.as_ref(),
        }
    }

    pub fn take_receiver(&mut self, job_id: u32) -> Option<flume::Receiver<JobEvent>> {
        let meta = self.jobs.get_mut(&job_id)?;
        meta.event_rx.take()
    }

    pub fn drain_events(&self, buf: &mut Vec<(u32, JobEvent)>) {
        buf.clear();
        for (&id, meta) in &self.jobs {
            if let Some(ref rx) = meta.event_rx {
                while let Ok(event) = rx.try_recv() {
                    buf.push((id, event));
                }
            }
        }
    }

    pub fn mark_dead(&mut self, job_id: u32) {
        if let Some(meta) = self.jobs.get_mut(&job_id) {
            meta.alive = false;
        }
    }

    pub fn write(&self, job_id: u32, data: String) -> Result<(), String> {
        let meta = self.jobs.get(&job_id).ok_or_else(|| {
            format!("job {} not found", job_id)
        })?;
        let tx = meta.stdin_tx.as_ref().ok_or_else(|| {
            format!("job {} does not accept stdin", job_id)
        })?;
        tx.send(data).map_err(|_| "job stdin closed".to_string())
    }

    pub fn close_stdin(&mut self, job_id: u32) {
        if let Some(meta) = self.jobs.get_mut(&job_id) {
            meta.stdin_tx.take();
        }
    }

    pub fn kill(&mut self, job_id: u32) {
        if let Some(meta) = self.jobs.get_mut(&job_id)
            && meta.alive
        {
            kill_job(meta);
        }
    }

    pub fn kill_all(&mut self) {
        for meta in self.jobs.values_mut() {
            if meta.alive {
                kill_job(meta);
            }
        }
    }

    pub fn clear(&mut self, lua: &Lua) {
        for (_, meta) in self.jobs.drain() {
            for key in [
                meta.on_stdout,
                meta.on_stderr,
                meta.on_stdout_chunk,
                meta.on_stderr_chunk,
                meta.on_exit,
            ]
            .into_iter()
            .flatten()
            {
                lua.remove_registry_value(key).ok();
            }
        }
    }
}

fn shell_command(cmd: &str) -> Command {
    #[cfg(unix)]
    {
        let mut c = Command::new("bash");
        c.arg("-c").arg(cmd);
        c
    }
    #[cfg(windows)]
    {
        let mut c = Command::new("cmd.exe");
        c.arg("/C").arg(cmd);
        c
    }
}

fn kill_job(meta: &mut JobMeta) {
    match meta.kind {
        JobKind::Process { pid } => {
            meta.stdin_tx.take();
            #[cfg(unix)]
            unsafe {
                libc::killpg(pid as libc::pid_t, libc::SIGKILL);
            }
            #[cfg(windows)]
            {
                const PROCESS_TERMINATE: u32 = 0x0001;
                unsafe extern "system" {
                    fn OpenProcess(access: u32, inherit: i32, pid: u32) -> *mut std::ffi::c_void;
                    fn TerminateProcess(handle: *mut std::ffi::c_void, exit_code: u32) -> i32;
                    fn CloseHandle(handle: *mut std::ffi::c_void) -> i32;
                }
                unsafe {
                    let handle = OpenProcess(PROCESS_TERMINATE, 0, pid);
                    if !handle.is_null() {
                        TerminateProcess(handle, 1);
                        CloseHandle(handle);
                    }
                }
            }
        }
    }
}

pub(crate) fn create_fn_table(lua: &Lua, perms: &PluginPermissions) -> LuaResult<Table> {
    let t = lua.create_table()?;

    t.set(
        "jobstart",
        perms.guard(Run, lua, |lua, (cmd, opts): (String, Option<Table>)| {
            let (cwd, env, use_stdin, on_stdout, on_stderr, on_stdout_chunk, on_stderr_chunk, on_exit) =
                match opts {
                    Some(ref opts) => {
                        let cwd: Option<String> = opts.get("cwd").ok();
                        let env: Option<HashMap<String, String>> = opts
                            .get::<Table>("env")
                            .ok()
                            .map(|t| t.pairs::<String, String>().filter_map(Result::ok).collect());
                        let use_stdin: bool = opts.get("stdin").unwrap_or(false);
                        let on_stdout = opts
                            .get::<Function>("on_stdout")
                            .ok()
                            .map(|f| lua.create_registry_value(f))
                            .transpose()?;
                        let on_stderr = opts
                            .get::<Function>("on_stderr")
                            .ok()
                            .map(|f| lua.create_registry_value(f))
                            .transpose()?;
                        let on_stdout_chunk = opts
                            .get::<Function>("on_stdout_chunk")
                            .ok()
                            .map(|f| lua.create_registry_value(f))
                            .transpose()?;
                        let on_stderr_chunk = opts
                            .get::<Function>("on_stderr_chunk")
                            .ok()
                            .map(|f| lua.create_registry_value(f))
                            .transpose()?;
                        let on_exit = opts
                            .get::<Function>("on_exit")
                            .ok()
                            .map(|f| lua.create_registry_value(f))
                            .transpose()?;
                        (cwd, env, use_stdin, on_stdout, on_stderr, on_stdout_chunk, on_stderr_chunk, on_exit)
                    }
                    None => (None, None, false, None, None, None, None, None),
                };

            with_task_jobs(lua, |store| {
                store.start(&cmd, cwd, env, use_stdin, on_stdout, on_stderr, on_stdout_chunk, on_stderr_chunk, on_exit)
            })
            .map_err(mlua::Error::runtime)
        })?,
    )?;

    t.set(
        "jobstop",
        perms.guard(Run, lua, |lua, job_id: u32| {
            with_task_jobs(lua, |store| store.kill(job_id));
            Ok(())
        })?,
    )?;

    t.set(
        "jobwrite",
        perms.guard(Run, lua, |lua, (job_id, data): (u32, String)| {
            with_task_jobs(lua, |store| store.write(job_id, data))
                .map_err(mlua::Error::runtime)
        })?,
    )?;

    t.set(
        "jobclose",
        perms.guard(Run, lua, |lua, job_id: u32| {
            with_task_jobs(lua, |store| store.close_stdin(job_id));
            Ok(())
        })?,
    )?;

    t.set(
        "jobwait",
        perms.guard_async(
            Run,
            lua,
            |lua, (job_id, timeout_ms): (u32, Option<u64>)| async move {
                let rx = with_task_jobs(&lua, |store| store.take_receiver(job_id))
                    .ok_or_else(|| mlua::Error::runtime("unknown job id or already waited"))?;

                let timeout = Duration::from_millis(timeout_ms.unwrap_or(30_000));
                let deadline = smol::Timer::after(timeout);
                futures_lite::pin!(deadline);

                let mut stdout_lines = Vec::new();
                let mut stderr_lines = Vec::new();

                let exit_code = loop {
                    let event =
                        futures_lite::future::or(async { rx.recv_async().await.ok() }, async {
                            (&mut deadline).await;
                            None
                        })
                        .await;

                    match event {
                        None => return Ok(mlua::Value::Nil),
                        Some(JobEvent::Stdout(line)) | Some(JobEvent::StdoutChunk(line)) => stdout_lines.push(line),
                        Some(JobEvent::Stderr(line)) | Some(JobEvent::StderrChunk(line)) => stderr_lines.push(line),
                        Some(JobEvent::Exit(code)) => {
                            break code;
                        }
                    }
                };

                let result = lua.create_table()?;
                result.set("stdout", stdout_lines.join("\n"))?;
                result.set("stderr", stderr_lines.join("\n"))?;
                result.set("exit_code", exit_code)?;
                Ok(mlua::Value::Table(result))
            },
        )?,
    )?;

    t.set(
        "executable",
        perms.guard(Env, lua, |_, name: String| {
            let found = env::var_os("PATH")
                .map(|paths| env::split_paths(&paths).any(|dir| dir.join(&name).is_file()))
                .unwrap_or(false)
                || Path::new(&name).is_file();
            Ok(if found { 1 } else { 0 })
        })?,
    )?;

    Ok(t)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_store() -> JobStore {
        JobStore::new()
    }

    fn start_echo(store: &mut JobStore) -> u32 {
        store
            .start("echo hello", None, None, false, None, None, None, None, None)
            .unwrap()
    }

    #[test]
    fn start_invalid_cwd_returns_error() {
        let mut store = make_store();
        let result = store.start(
            "echo hello",
            Some("/nonexistent_dir_abc_xyz_123".into()),
            None,
            false,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn has_alive_jobs_tracks_state() {
        let mut store = make_store();
        assert!(!store.has_alive_jobs());

        let id = start_echo(&mut store);
        assert!(store.has_alive_jobs());

        store.mark_dead(id);
        assert!(!store.has_alive_jobs());
    }

    #[test]
    fn noop_on_nonexistent_or_dead_jobs() {
        let mut store = make_store();
        store.mark_dead(999);
        store.kill(999);

        let id = start_echo(&mut store);
        store.mark_dead(id);
        store.kill(id);

        assert!(store.callback_key(999, &JobEvent::Exit(0)).is_none());
    }

    #[test]
    fn take_receiver_lifecycle() {
        let mut store = make_store();
        assert!(store.take_receiver(999).is_none());

        let id = start_echo(&mut store);
        assert!(store.take_receiver(id).is_some());
        assert!(
            store.take_receiver(id).is_none(),
            "second take should fail (receiver already moved)"
        );
    }

    #[test]
    fn callback_key_returns_none_without_callbacks() {
        let mut store = make_store();
        let id = start_echo(&mut store);
        assert!(
            store
                .callback_key(id, &JobEvent::Stdout("x".into()))
                .is_none()
        );
        assert!(
            store
                .callback_key(id, &JobEvent::Stderr("x".into()))
                .is_none()
        );
        assert!(store.callback_key(id, &JobEvent::Exit(0)).is_none());
    }

    #[test]
    fn take_receiver_delivers_events() {
        let mut store = make_store();
        let id = start_echo(&mut store);
        let rx = store.take_receiver(id).unwrap();

        let mut got_exit = false;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            match rx.recv_timeout(Duration::from_millis(200)) {
                Ok(JobEvent::Exit(_)) => {
                    got_exit = true;
                    break;
                }
                Ok(_) => continue,
                Err(flume::RecvTimeoutError::Timeout) => continue,
                Err(flume::RecvTimeoutError::Disconnected) => break,
            }
        }
        assert!(got_exit, "should receive exit event for completed job");
    }

    #[test]
    fn drain_events_collects_from_all_jobs() {
        let mut store = make_store();
        let id = start_echo(&mut store);

        let mut buf = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            store.drain_events(&mut buf);
            if buf
                .iter()
                .any(|(jid, e)| *jid == id && matches!(e, JobEvent::Exit(_)))
            {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!("should receive exit event for completed job");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    #[test]
    fn drain_events_empty_after_take() {
        let mut store = make_store();
        let id = start_echo(&mut store);
        let _rx = store.take_receiver(id).unwrap();

        let mut buf = Vec::new();
        store.drain_events(&mut buf);
        assert!(
            buf.is_empty(),
            "drained receiver yields no events via drain_events"
        );
    }

    #[test]
    fn write_to_job_without_stdin_returns_error() {
        let mut store = make_store();
        let id = start_echo(&mut store);
        let err = store.write(id, "hello".into()).unwrap_err();
        assert!(err.contains("does not accept stdin"), "{err}");
    }

    #[test]
    fn write_to_nonexistent_job_returns_error() {
        let store = make_store();
        let err = store.write(999, "hello".into()).unwrap_err();
        assert!(err.contains("not found"), "{err}");
    }

    #[test]
    fn write_to_job_with_stdin_delivers_data() {
        let mut store = make_store();
        let id = store
            .start("cat", None, None, true, None, None, None, None, None)
            .unwrap();

        store.write(id, "hello stdin\n".into()).unwrap();
        store.close_stdin(id);

        let rx = store.take_receiver(id).unwrap();
        let mut got_output = false;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            match rx.recv_timeout(Duration::from_millis(200)) {
                Ok(JobEvent::Stdout(line)) if line == "hello stdin" => {
                    got_output = true;
                    break;
                }
                Ok(_) => continue,
                Err(flume::RecvTimeoutError::Timeout) => continue,
                Err(flume::RecvTimeoutError::Disconnected) => break,
            }
        }
        assert!(got_output, "cat should echo stdin to stdout");
    }

    #[test]
    fn write_after_kill_returns_error() {
        let mut store = make_store();
        let id = store
            .start("sleep 10", None, None, true, None, None, None, None, None)
            .unwrap();

        store.write(id, "before".into()).unwrap();
        store.kill(id);

        let result = store.write(id, "after".into());
        assert!(result.is_err());
    }
}
