// wa-runner.cpp — single-runner for all peers via global events log
// - Tails wa-hub events (global or per-peer) using wa-sub
// - Parses slash-commands and executes whitelisted commands from commands.json
// - Replies via wa-hub FIFO
// Build: g++ -std=c++20 -O2 wa-runner.cpp -o wa-runner

#include <nlohmann/json.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <regex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#if defined(__unix__) || defined(__APPLE__)
  #include <unistd.h>
  #include <sys/wait.h>
  #include <sys/select.h>
#endif

using json = nlohmann::json;
namespace fs = std::filesystem;

static const char* kVersion = "wa-runner 1.3";

static std::atomic<bool> g_running{true};

static long long now_ms(){
  using namespace std::chrono;
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

static std::string getenv_s(const char* k){ const char* v=getenv(k); return v?std::string(v):std::string(); }

static json load_json_file(const fs::path& p){
  std::ifstream f(p);
  if(!f.good()) return json::object();
  try { json j; f>>j; return j; } catch(...) { return json::object(); }
}

// ------- help -------
static void print_help_long(){
  std::cout << R"(wa-runner — execute whitelisted commands on incoming WhatsApp messages
Version: )" << kVersion << R"(

SUMMARY
  Tails wa-hub JSONL events using wa-sub. For each inbound text that starts with a
  slash-command, looks up a command template in commands.json, executes it, and
  optionally replies with output through wa-hub's FIFO.

USAGE (choose one source)
  Global events file (all peers):
    wa-runner --file /path/to/events.jsonl --wa-sub /usr/local/bin/wa-sub \
              --commands commands.json [--fifo /path/send.fifo] [--auto-reply] \
              [--log-dir DIR] [--log-prefix PFX] [--log-ext EXT] \
              [--cmd-timeout SEC] [--debug]

  Single peer (legacy mode):
    wa-runner --peer NAME --config /path/wa-hub.json --wa-sub /usr/local/bin/wa-sub \
              --commands commands.json [--fifo /path/send.fifo] [--auto-reply] \
              [--log-dir DIR] [--log-prefix PFX] [--log-ext EXT] \
              [--cmd-timeout SEC] [--debug]

OPTIONS
  --file PATH              Global events JSONL written by wa-hub (covers all peers).
  --peer NAME              Subscribe only to this peer’s per-file events (requires --config).
  --config PATH            wa-hub.json path (used to locate per-peer file when --peer is used).
  --wa-sub PATH            Path to wa-sub binary.
  --commands PATH          Command map JSON file (templates). See “COMMAND MAP JSON”.
  --fifo PATH              wa-hub send FIFO. When set with --auto-reply, replies via FIFO.
  --auto-reply             After a command runs, reply with “ok <cmd> rc=<code>” and
                           up to 800 chars of stdout.
  --cmd-timeout SEC        Kill a command after SEC seconds. Default 30.
  --log-dir DIR            Runner log directory. Default ./runner-logs.
  --log-prefix PFX         Filename prefix for per-peer runner logs. Default runner_
  --log-ext EXT            Filename extension for runner logs. Default .jsonl
  --debug                  Print resolved wa-sub command and other diagnostics to stderr.
  --help                   This help.
  --version                Print version.

CONFIG FALLBACKS (wa-hub.json optional keys)
  When --config is provided, the following keys are read unless overridden by CLI:
    "runner_log_dir":   "/abs/or/relative/dir",
    "runner_log_prefix":"runner_",
    "runner_log_ext":   ".jsonl"

EVENT FORMAT (input from wa-sub)
  Each line is a JSON object. Only events with {"kind":"received"} are considered.
  Minimal fields:
    {"kind":"received","peer":"<alias|number>","text":"<incoming message>","ts":<ms>}

COMMAND TRIGGER SYNTAX
  Incoming message must start with a slash:
    /name               no arguments
    /name arg tail      argument tail preserved verbatim after first space
  Name chars allowed in command: [A-Za-z0-9_-]. The remainder becomes the argument tail.

COMMAND MAP JSON (templates)
  Structure:
  {
    "global": {
      "echo": ["/usr/bin/printf", "%s", "{args}"],
      "uptime": ["/usr/bin/uptime"]
    },
    "max": {
      "tail": ["/usr/bin/tail","-n","20","/var/log/syslog"]
    }
  }
  Resolution order: peer block first, then "global".
  Template tokens:
    {args}   — pass the entire argument tail as a single argv element (spaces preserved)
    {args*}  — shlex-split the argument tail into multiple argv elements

SECURITY NOTES
  • Only whitelisted commands in commands.json are runnable.
  • Prefer absolute paths in templates. Avoid invoking shells unless necessary.
  • Runner logs each execution to <log-dir>/<prefix><peer><ext> as JSONL with:
      {"ts":..., "peer":"...", "incoming":"/cmd ...", "cmd":"...", "argv":[...],
       "args":"...", "rc":int, "stdout":"...", "stderr":"..."}

EXIT CODES
  0  Normal exit (signal or EOF from wa-sub).
  1  System/exec pipe or spawn error.
  2  Bad usage.

EXAMPLES
  # 1) Single runner for all peers via global events
  wa-runner --file /home/kidders/nas/var/wa-hub/events.jsonl \
            --wa-sub /usr/local/bin/wa-sub \
            --commands /home/kidders/apps/wa-hub/config/commands.json \
            --fifo /home/kidders/var/wa-hub/send.fifo \
            --auto-reply --log-dir /home/kidders/var/wa-runner --cmd-timeout 30

  # 2) Legacy: one runner per peer
  wa-runner --peer max --config /home/kidders/apps/wa-hub/config/wa-hub.json \
            --wa-sub /usr/local/bin/wa-sub \
            --commands /home/kidders/apps/wa-hub/config/commands.json \
            --fifo /home/kidders/var/wa-hub/send.fifo --auto-reply

  # 3) Commands JSON snippet
  {
    "global": {
      "echo":   ["/usr/bin/printf","%s","{args}"],
      "say":    ["/usr/bin/espeak","{args*}"],
      "uptime": ["/usr/bin/uptime"]
    },
    "max": {
      "tail":   ["/usr/bin/tail","-n","100","/var/log/syslog"]
    }
  }

  # 4) From WhatsApp send:
    /echo Hello World
    /say 'quoted arg'  another
    /uptime

  # 5) With auto-reply, runner will send back “ok <cmd> rc=<code>” and a snippet of stdout.

SYSTEMD (user) quick sketch
  ~/.config/systemd/user/wa-runner.service
    [Unit]
    Description=WA Runner (all peers)
    After=wa-hub.service
    Wants=wa-hub.service

    [Service]
    Type=simple
    ExecStart=/usr/local/bin/wa-runner --file /home/USER/nas/var/wa-hub/events.jsonl \
              --wa-sub /usr/local/bin/wa-sub \
              --commands /home/USER/apps/wa-hub/config/commands.json \
              --fifo /home/USER/var/wa-hub/send.fifo \
              --auto-reply --log-dir /home/USER/var/wa-runner
    Restart=always
    RestartSec=2

    [Install]
    WantedBy=default.target

TROUBLESHOOTING
  • No output? Run with --debug and verify the spawned wa-sub command.
  • Ensure events.jsonl is being appended by wa-hub and readable by this process.
  • Replies require FIFO path and a running wa-hub with an open FIFO reader.

)";
}

// ------- argv split -------
static std::vector<std::string> shlex_split(const std::string& s){
  std::vector<std::string> out;
  std::string cur; bool in_s=false, in_d=false;
  for(size_t i=0;i<s.size();++i){
    char c=s[i];
    if(c=='\'' && !in_d){ in_s=!in_s; continue; }
    if(c=='\"' && !in_s){ in_d=!in_d; continue; }
    if(std::isspace((unsigned char)c) && !in_s && !in_d){
      if(!cur.empty()){ out.push_back(cur); cur.clear(); }
      continue;
    }
    cur.push_back(c);
  }
  if(!cur.empty()) out.push_back(cur);
  return out;
}

// Build argv from template; `{args}` keeps spaces as one arg; `{args*}` splits
static std::vector<std::string> build_argv(const std::vector<std::string>& tmpl,
                                           const std::string& argline){
  std::vector<std::string> argv;
  for(const auto& tok : tmpl){
    if(tok == "{args}") {
      argv.push_back(argline); // preserve spaces exactly
    } else if(tok == "{args*}") {
      auto pieces = shlex_split(argline);
      argv.insert(argv.end(), pieces.begin(), pieces.end());
    } else if(tok.find("{args}") != std::string::npos) {
      std::string t = tok;
      size_t pos = 0;
      while((pos = t.find("{args}", pos)) != std::string::npos){
        t.replace(pos, 6, argline);
        pos += argline.size();
      }
      argv.push_back(std::move(t));
    } else {
      argv.push_back(tok);
    }
  }
  return argv;
}

static int run_argv(const std::vector<std::string>& argv, std::string& out, std::string& err, int timeout_sec){
#if defined(__unix__) || defined(__APPLE__)
  int out_pipe[2], err_pipe[2];
  if(pipe(out_pipe)!=0 || pipe(err_pipe)!=0) return 1;

  pid_t pid=fork();
  if(pid<0) return 1;

  if(pid==0){
    dup2(out_pipe[1], STDOUT_FILENO);
    dup2(err_pipe[1], STDERR_FILENO);
    close(out_pipe[0]); close(out_pipe[1]);
    close(err_pipe[0]); close(err_pipe[1]);

    std::vector<char*> cargv;
    cargv.reserve(argv.size()+1);
    for(const auto& s: argv) cargv.push_back(const_cast<char*>(s.c_str()));
    cargv.push_back(nullptr);

    execvp(cargv[0], cargv.data());
    std::perror("execvp");
    _exit(127);
  }

  close(out_pipe[1]); close(err_pipe[1]);

  auto read_fd = [](int fd, std::string& dst){
    char buf[4096];
    ssize_t r;
    while((r=read(fd,buf,sizeof(buf)))>0) dst.append(buf, buf+r);
  };

  auto t0 = std::chrono::steady_clock::now();
  int status = 0;
  bool child_done = false;

  while(!child_done){
    int r = waitpid(pid, &status, WNOHANG);
    if(r == pid){
      child_done = true;
      break;
    }
    fd_set set;
    FD_ZERO(&set);
    FD_SET(out_pipe[0], &set);
    FD_SET(err_pipe[0], &set);
    timeval tv{0,200*1000}; // 200ms
    select(std::max(out_pipe[0],err_pipe[0])+1, &set, nullptr, nullptr, &tv);
    if(FD_ISSET(out_pipe[0], &set)) read_fd(out_pipe[0], out);
    if(FD_ISSET(err_pipe[0], &set)) read_fd(err_pipe[0], err);

    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now()-t0).count();
    if(timeout_sec>0 && elapsed >= timeout_sec){
      kill(pid, SIGKILL);
      waitpid(pid, &status, 0);
      break;
    }
    if(!FD_ISSET(out_pipe[0], &set) && !FD_ISSET(err_pipe[0], &set)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  }
  { std::string tmp; read_fd(out_pipe[0], tmp); out+=tmp; }
  { std::string tmp; read_fd(err_pipe[0], tmp); err+=tmp; }
  close(out_pipe[0]); close(err_pipe[0]);

  if(WIFEXITED(status)) return WEXITSTATUS(status);
  return 128;
#else
  (void)argv; (void)out; (void)err; (void)timeout_sec;
  return 127;
#endif
}

static bool fifo_send(const fs::path& fifo, const std::string& peer, const std::string& text){
  json msg = {{"to",peer},{"text",text}};
  std::ofstream f(fifo);
  if(!f.good()) return false;
  f<<msg.dump()<<'\n';
  return true;
}

static void on_sigint(int){ g_running=false; }

// ---------------- main ----------------
int main(int argc, char** argv){
  // args
  fs::path wa_sub = "wa-sub";
  fs::path cfg = "wa-hub.json";
  std::string peer;            // optional if --file is used
  fs::path file;               // global events.jsonl to cover all peers
  fs::path commands = "commands.json";
  fs::path fifo;

  fs::path log_dir = "./runner-logs";
  std::string log_prefix = "runner_";
  std::string log_ext = ".jsonl";

  bool auto_reply=false;
  bool debug=false;
  int timeout_sec=30;

  bool cli_log_dir=false, cli_log_prefix=false, cli_log_ext=false;

  // parse
  for(int i=1;i<argc;i++){
    std::string s=argv[i];
    auto need=[&](const char* k){ if(i+1>=argc){ std::cerr<<"missing "<<k<<"\n"; return 2; } return 0; };
    if(s=="--peer"){ if(need("--peer")) return 2; peer=argv[++i]; }
    else if(s=="--file"){ if(need("--file")) return 2; file=argv[++i]; }
    else if(s=="--wa-sub"){ if(need("--wa-sub")) return 2; wa_sub=argv[++i]; }
    else if(s=="--config"){ if(need("--config")) return 2; cfg=argv[++i]; }
    else if(s=="--commands"){ if(need("--commands")) return 2; commands=argv[++i]; }
    else if(s=="--fifo"){ if(need("--fifo")) return 2; fifo=argv[++i]; }
    else if(s=="--log-dir"){ if(need("--log-dir")) return 2; log_dir=argv[++i]; cli_log_dir=true; }
    else if(s=="--log-prefix"){ if(need("--log-prefix")) return 2; log_prefix=argv[++i]; cli_log_prefix=true; }
    else if(s=="--log-ext"){ if(need("--log-ext")) return 2; log_ext=argv[++i]; cli_log_ext=true; }
    else if(s=="--cmd-timeout"){ if(need("--cmd-timeout")) return 2; timeout_sec=std::stoi(argv[++i]); }
    else if(s=="--auto-reply"){ auto_reply=true; }
    else if(s=="--debug"){ debug=true; }
    else if(s=="--help"){ print_help_long(); return 0; }
    else if(s=="--version"){ std::cout<<kVersion<<"\n"; return 0; }
    else { std::cerr<<"unknown arg "<<s<<"\n\n"; print_help_long(); return 2; }
  }

  if(!file.empty() && !peer.empty()){ std::cerr<<"choose one of --file or --peer\n\n"; print_help_long(); return 2; }
  if(file.empty() && peer.empty()){ std::cerr<<"--file or --peer required\n\n"; print_help_long(); return 2; }

  // apply log config from wa-hub.json if present (only for values not set by CLI)
  if(!cfg.empty()){
    json j = load_json_file(cfg);
    if(j.is_object()){
      if(!cli_log_dir && j.contains("runner_log_dir") && j["runner_log_dir"].is_string())
        log_dir = j["runner_log_dir"].get<std::string>();
      if(!cli_log_prefix && j.contains("runner_log_prefix") && j["runner_log_prefix"].is_string())
        log_prefix = j["runner_log_prefix"].get<std::string>();
      if(!cli_log_ext && j.contains("runner_log_ext") && j["runner_log_ext"].is_string())
        log_ext = j["runner_log_ext"].get<std::string>();
    }
  }

  std::error_code ec; fs::create_directories(log_dir, ec);

  // load commands map
  json cmdmap = load_json_file(commands);
  if(cmdmap.is_null() || !cmdmap.is_object()){
    std::cerr<<"commands file invalid or missing: "<<commands<<"\n";
    return 2;
  }
  if(debug) std::cerr<<"loaded commands keys: "<<cmdmap.size()<<"\n";

  // build wa-sub command
  std::vector<std::string> sub_argv;
  if(!file.empty()){
    sub_argv = { wa_sub.string(), "--file", file.string(), "--kind", "received", "--follow" };
  } else {
    sub_argv = { wa_sub.string(), "--peer", peer, "--kind", "received", "--follow", "--config", cfg.string() };
  }

  if(debug){
    std::cerr<<"spawn: ";
    for(auto& s: sub_argv) std::cerr<<s<<" ";
    std::cerr<<"\n";
  }

#if defined(__unix__) || defined(__APPLE__)
  int pipefd[2];
  if(pipe(pipefd)!=0){ std::perror("pipe"); return 1; }
  pid_t pid=fork();
  if(pid<0){ std::perror("fork"); return 1; }
  if(pid==0){
    // child: wa-sub -> stdout -> pipe
    dup2(pipefd[1], STDOUT_FILENO);
    close(pipefd[0]); close(pipefd[1]);
    std::vector<char*> cargv; cargv.reserve(sub_argv.size()+1);
    for(auto& s: sub_argv) cargv.push_back(const_cast<char*>(s.c_str()));
    cargv.push_back(nullptr);
    execvp(cargv[0], cargv.data());
    std::perror("execvp wa-sub");
    _exit(127);
  }
  // parent:
  close(pipefd[1]);

  signal(SIGINT, on_sigint);
  signal(SIGTERM, on_sigint);

  FILE* in = fdopen(pipefd[0], "r");
  if(!in){ std::perror("fdopen"); return 1; }

  char* line=nullptr; size_t n=0;
  while(g_running){
    ssize_t r=getline(&line,&n,in);
    if(r<=0){ if(feof(in)) break; clearerr(in); std::this_thread::sleep_for(std::chrono::milliseconds(50)); continue; }
    std::string raw(line, r);

    // wa-sub prints one JSON obj per line
    json ev = json::parse(raw, nullptr, false);
    if(ev.is_discarded()) continue;
    if(ev.value("kind", std::string())!="received") continue;

    // route by peer from each event
    std::string peer_in = ev.value("peer", peer);
    std::string text = ev.value("text", std::string());
    long long ts = ev.value("ts", now_ms());
    if(text.empty()) continue;

    if(text[0] != '/') continue;

    // parse /cmd and RAW arg tail (preserve spaces)
    std::string name, argline;
    {
      std::string body = text.substr(1);
      size_t j = 0;
      while(j<body.size()){
        unsigned char c = (unsigned char)body[j];
        if(!(std::isalnum(c) || c=='_' || c=='-')) break;
        ++j;
      }
      name = body.substr(0,j);
      size_t k = j;
      if(k < body.size() && std::isspace((unsigned char)body[k])) ++k;
      if(k < body.size()) argline = body.substr(k);
      while(!argline.empty() && (argline.back()=='\n' || argline.back()=='\r')) argline.pop_back();
    }

    // resolve mapping: peer-specific first, then global
    json peer_map = (cmdmap.contains(peer_in) && cmdmap[peer_in].is_object()) ? cmdmap[peer_in] : json::object();
    json mapping = (peer_map.contains(name) ? peer_map[name]
                   : (cmdmap.contains("global") && cmdmap["global"].contains(name) ? cmdmap["global"][name] : json()));

    fs::path logf = log_dir / (log_prefix + peer_in + log_ext);

    if(mapping.is_null() || !mapping.is_array() || mapping.empty()){
      json rec = {{"ts",ts},{"peer",peer_in},{"incoming",text},{"cmd",name},{"rc",-1},{"stderr","unknown command"}};
      std::ofstream lf(logf, std::ios::app); lf<<rec.dump()<<'\n';
      continue;
    }

    std::vector<std::string> tmpl;
    for(auto& v: mapping) if(v.is_string()) tmpl.push_back(v.get<std::string>());
    auto argv_run = build_argv(tmpl, argline);

    std::string sout, serr;
    int rc = run_argv(argv_run, sout, serr, timeout_sec);

    json rec = {
      {"ts",ts},{"peer",peer_in},{"incoming",text},{"cmd",name},
      {"argv",tmpl},{"args",argline},{"rc",rc},
      {"stdout",sout},{"stderr",serr}
    };
    { std::ofstream lf(logf, std::ios::app); lf<<rec.dump()<<'\n'; }

    if(auto_reply && !fifo.empty()){
      std::ostringstream reply;
      reply<<"ok "<<name<<" rc="<<rc;
      if(!sout.empty()){
        std::string cut = sout.substr(0, std::min<size_t>(800, sout.size()));
        cut.erase(std::remove(cut.begin(), cut.end(), '\r'), cut.end());
        if(!cut.empty() && cut.back()=='\n') cut.pop_back();
        reply<<"\n"<<cut;
      }
      fifo_send(fifo, peer_in, reply.str());
    }
  }
  if(line) free(line);
  fclose(in);
  int st=0; waitpid(pid,&st,0);
#else
  std::cerr<<"wa-runner only implemented on Unix-like systems.\n";
  return 1;
#endif
  return 0;
}
