// wa-sub.cpp  v1.4 — NAS-safe tail, alias-aware peer resolution, configurable dirs/names
#include <nlohmann/json.hpp>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <regex>
#include <string>
#include <vector>

using json = nlohmann::json;
namespace fs = std::filesystem;

static long long now_ms(){
  using namespace std::chrono;
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

// ---------- cfg ----------
struct HubCfg {
  fs::path base_dir;
  fs::path data_dir;
  fs::path aliases_path;

  fs::path global_dir;
  fs::path per_dir;
  std::string global_name = "events.jsonl";
  std::string per_prefix = "events.";
  std::string per_suffix = ".jsonl";

  // legacy
  std::string legacy_global_log;
};

static std::string getenv_s(const char* k){ const char* v=getenv(k); return v?std::string(v):std::string(); }

static HubCfg load_hub_cfg(const fs::path& cfg_path_in){
  HubCfg c;
  fs::path home = getenv_s("HOME").empty()? "." : fs::path(getenv_s("HOME"));
  c.base_dir = home / ".wa-hub";
  c.data_dir.clear();
  c.aliases_path = c.base_dir / "aliases.json";

  fs::path cfg_path = cfg_path_in;
  if(cfg_path.empty()){
    std::string env_cfg = getenv_s("WA_HUB_CONFIG");
    if(!env_cfg.empty()) cfg_path = env_cfg;
    else if(fs::exists(home/".wa-hub/wa-hub.json")) cfg_path = home/".wa-hub/wa-hub.json";
    else if(fs::exists(fs::current_path()/ "wa-hub.json")) cfg_path = fs::current_path()/ "wa-hub.json";
  }
  fs::path cfg_dir  = cfg_path.empty()? fs::current_path() : cfg_path.parent_path();

  if(!cfg_path.empty()){
    std::ifstream f(cfg_path);
    if(f.good()){
      try{
        json j; f>>j;
        auto P=[&](const char* k, fs::path& v){
          if(j.contains(k)){
            fs::path tmp = j[k].get<std::string>();
            v = tmp.is_absolute()? tmp : (cfg_dir / tmp);
          }
        };
        auto S=[&](const char* k, std::string& v){ if(j.contains(k)) v=j[k].get<std::string>(); };

        P("base_dir", c.base_dir);
        P("data_dir", c.data_dir);
        P("aliases_path", c.aliases_path);

        P("global_dir", c.global_dir);
        P("per_dir",    c.per_dir);
        S("global_name", c.global_name);
        S("per_prefix",  c.per_prefix);
        S("per_suffix",  c.per_suffix);

        S("global_log", c.legacy_global_log);
      }catch(...){}
    }
  }

  if(c.data_dir.empty()) c.data_dir = c.base_dir;
  if(c.global_dir.empty()) c.global_dir = c.data_dir;
  if(c.per_dir.empty())    c.per_dir    = c.data_dir;

  if(!c.legacy_global_log.empty()){
    fs::path gl = c.legacy_global_log;
    if(gl.has_parent_path()){
      c.global_dir = gl.parent_path();
      c.global_name = gl.filename().string();
    } else {
      c.global_name = gl.string();
    }
  }

  if(!c.aliases_path.is_absolute()) c.aliases_path = cfg_dir / c.aliases_path;
  return c;
}

// ---------- args/help ----------
struct Args{
  fs::path file;
  std::string peer;
  fs::path cfg;
  std::optional<std::string> kind;     // received|sent|status
  std::optional<std::string> grep_pat; // regex on .text (use (?i) prefix for case-insensitive)
  std::optional<long long> since_ts;   // epoch ms
  bool follow=false, once=false, json_array=false, debug=false, help=false;
  std::optional<int> window_sec;
  std::optional<int> timeout_sec;
};

static void print_help(){
  std::cout <<
R"(wa-sub v1.4 — tail and filter wa-hub JSONL logs

USAGE
  wa-sub --file <path> | --peer <name|number> [--config <wa-hub.json>]
         [--kind received|sent|status] [--grep <regex>] [--since-ts <epoch_ms>]
         (--follow | --once --timeout <sec> | --window <sec> [--json-array])
         [--debug] [--help]

SOURCES
  --file PATH                    Read this JSONL file directly.
  --peer NAME|NUMBER --config CFG
                                 Resolve to per-peer file using CFG:
                                   tail (per_dir)/(per_prefix + KEY + per_suffix)
                                 If NUMBER matches an alias in aliases_path, KEY is that alias.

FILTERS
  --kind received|sent|status    Only those event kinds.
  --grep REGEX                   Match .text with REGEX. Prefix (?i) for case-insensitive.
  --since-ts MS                  Only events with ts >= MS (epoch milliseconds).

MODES (choose exactly one)
  --follow                       Stream new matching lines until Ctrl-C.
  --once --timeout S             Exit on first matching line or after S seconds (exit code 1 on timeout).
  --window S [--json-array]      Collect for S seconds, then exit. With --json-array prints one JSON array.

OTHER
  --config CFG                   Path to wa-hub.json (for --peer). If omitted, tries:
                                   $WA_HUB_CONFIG, ~/.wa-hub/wa-hub.json, ./wa-hub.json
  --json-array                   Buffer matched lines and print as a single JSON array (for --window/--once).
  --debug                        Print the resolved file path to stderr.
  --help                         This help.

EXIT CODES
  0  success (match found or normal window/follow exit)
  1  --once timeout elapsed without a match
  2  bad usage or fatal error
)";
}

static void die_usage(const std::string& m){
  std::cerr << m << "\n\n";
  print_help();
  std::exit(2);
}

static Args parse(int argc,char**argv){
  Args a;
  for(int i=1;i<argc;i++){
    std::string s=argv[i];
    auto need=[&](const char* opt){ if(i+1>=argc) die_usage(std::string("missing value for ")+opt); };
    if(s=="--help"){ a.help=true; }
    else if(s=="--file"){ need("--file"); a.file=argv[++i]; }
    else if(s=="--peer"){ need("--peer"); a.peer=argv[++i]; }
    else if(s=="--config"){ need("--config"); a.cfg=argv[++i]; }
    else if(s=="--kind"){ need("--kind"); a.kind=argv[++i]; }
    else if(s=="--grep"){ need("--grep"); a.grep_pat=argv[++i]; }
    else if(s=="--since-ts"){ need("--since-ts"); a.since_ts=std::stoll(argv[++i]); }
    else if(s=="--follow"){ a.follow=true; }
    else if(s=="--once"){ a.once=true; }
    else if(s=="--window"){ need("--window"); a.window_sec=std::stoi(argv[++i]); }
    else if(s=="--timeout"){ need("--timeout"); a.timeout_sec=std::stoi(argv[++i]); }
    else if(s=="--json-array"){ a.json_array=true; }
    else if(s=="--debug"){ a.debug=true; }
    else { die_usage(std::string("unknown arg: ")+s); }
  }

  if(a.help){ print_help(); std::exit(0); }

  int modes=(a.follow?1:0)+(a.once?1:0)+(a.window_sec?1:0);
  if(modes!=1) die_usage("choose exactly one mode: --follow OR --once --timeout S OR --window S");

  if(a.file.empty() && a.peer.empty()) die_usage("specify --file PATH or --peer NAME");
  if(a.once && !a.timeout_sec) die_usage("--once requires --timeout <sec>");
  if(a.kind && !(*a.kind=="received"||*a.kind=="sent"||*a.kind=="status"))
    die_usage("invalid --kind (use received|sent|status)");

  return a;
}

// ---------- filter ----------
struct Filter {
  std::optional<std::string> kind;
  std::optional<std::regex> re;
  std::optional<long long> since_ts;
};

static Filter make_filter(const Args& a){
  Filter f; f.kind=a.kind; f.since_ts=a.since_ts;
  if(a.grep_pat){
    std::regex::flag_type flags=std::regex::ECMAScript;
    std::string pat=*a.grep_pat;
    if(pat.rfind("(?i)",0)==0){ flags|=std::regex::icase; pat=pat.substr(4); }
    try{ f.re.emplace(pat, flags); }catch(const std::regex_error& e){
      die_usage(std::string("bad --grep regex: ")+e.what());
    }
  }
  return f;
}

static bool match_line(const std::string& raw, const Filter& f){
  json j=json::parse(raw,nullptr,false);
  if(j.is_discarded()) return false;
  if(f.kind && j.value("kind",std::string())!=*f.kind) return false;
  if(f.since_ts && j.value("ts",0LL) < *f.since_ts) return false;
  if(f.re){
    std::string t=j.value("text",std::string());
    if(!std::regex_search(t,*f.re)) return false;
  }
  return true;
}

// ---------- aliases ----------
static std::string map_number_to_alias(const fs::path& aliases_path, const std::string& in){
  std::ifstream f(aliases_path);
  if(!f.good()) return in;
  json j; try{ f>>j; }catch(...){ return in; }

  auto scan = [&](const json& obj)->std::string{
    for(auto it=obj.begin(); it!=obj.end(); ++it){
      if(it.value().is_string()){
        if(it.value().get<std::string>() == in) return it.key();
      }
    }
    return std::string();
  };

  if(j.is_object()){
    if(j.contains("aliases") && j["aliases"].is_object()){
      auto alias = scan(j["aliases"]);
      if(!alias.empty()) return alias;
    }else{
      auto alias = scan(j);
      if(!alias.empty()) return alias;
    }
  }
  return in;
}

// ---------- file utils ----------
static uint64_t inode_of(const fs::path& p){ struct stat st{}; return (::stat(p.c_str(),&st)==0)? st.st_ino : 0; }
static uint64_t size_of (const fs::path& p){ struct stat st{}; return (::stat(p.c_str(),&st)==0)? st.st_size: 0; }

int main(int argc,char**argv){
  Args a=parse(argc,argv);
  Filter filt=make_filter(a);

  fs::path target;
  if(!a.file.empty()) {
    target=a.file;
  } else {
    HubCfg c=load_hub_cfg(a.cfg);
    std::string key = map_number_to_alias(c.aliases_path, a.peer);
    target = (c.per_dir / (c.per_prefix + key + c.per_suffix));
  }

  if(a.debug) std::cerr<<"tailing: \""<<target.string()<<"\"\n";

  // wait for file in live modes
  while(!fs::exists(target)){
    if(!(a.follow||a.once||a.window_sec)) die_usage("file not found: "+target.string());
    usleep(200*1000);
  }

  uint64_t cur_inode = inode_of(target);
  uint64_t offset = 0;

  // starting position
  if(a.since_ts) offset = 0;           // need to scan history
  else           offset = size_of(target); // start at EOF by default

  std::vector<std::string> outbuf;
  auto emit = [&](const std::string& line){
    if(a.json_array) outbuf.push_back(line);
    else {
      std::cout<<line;
      if(line.empty() || line.back()!='\n') std::cout<<"\n";
      std::cout.flush();
    }
  };
  auto flush_array=[&](){
    if(a.json_array){
      std::cout<<"[";
      for(size_t i=0;i<outbuf.size();++i){
        if(i) std::cout<<",";
        std::cout<<outbuf[i];
      }
      std::cout<<"]\n";
      std::cout.flush();
    }
  };

  long long t0 = now_ms();
  long long deadline_once = a.once ? (t0 + (*a.timeout_sec*1000LL)) : LLONG_MAX;
  long long deadline_win  = a.window_sec ? (t0 + (*a.window_sec*1000LL)) : LLONG_MAX;

  // historical scan if since-ts
  if(a.since_ts){
    std::ifstream f(target);
    f.seekg(0, std::ios::beg);
    std::string line;
    while(std::getline(f,line)){
      if(match_line(line,filt)){
        emit(line+"\n");
        if(a.once){ flush_array(); return 0; }
      }
    }
    offset = (uint64_t)f.tellg();
    if((long long)offset<0) offset = size_of(target);
  }

  // main loop
  for(;;){
    if(a.once && now_ms()>=deadline_once){ flush_array(); return 1; }
    if(a.window_sec && now_ms()>=deadline_win){ flush_array(); return 0; }

    if(!fs::exists(target)){ usleep(200*1000); continue; }
    uint64_t ino = inode_of(target);
    uint64_t sz  = size_of(target);

    if(ino != cur_inode || sz < offset){ cur_inode = ino; offset = 0; }

    if(sz > offset){
      std::ifstream f(target);
      f.seekg((std::streamoff)offset, std::ios::beg);
      std::string line;
      while(std::getline(f,line)){
        offset = (uint64_t)f.tellg();
        if((long long)offset<0) offset = size_of(target);
        if(match_line(line,filt)){
          emit(line+"\n");
          if(a.once){ flush_array(); return 0; }
        }
      }
      continue;
    }
    usleep(200*1000);
  }
}
