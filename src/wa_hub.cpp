#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <fcntl.h>
#include <unistd.h>
#include <limits.h>

using json = nlohmann::json;
namespace fs = std::filesystem;

// ---------- HTTP ----------
static size_t sink(void* p,size_t s,size_t n,void* d){((std::string*)d)->append((char*)p,s*n);return s*n;}
static std::string http_get(const std::string& url,long* code=nullptr){
  CURL* c=curl_easy_init(); std::string out; if(!c) return out;
  curl_easy_setopt(c,CURLOPT_URL,url.c_str());
  curl_easy_setopt(c,CURLOPT_WRITEFUNCTION,sink);
  curl_easy_setopt(c,CURLOPT_WRITEDATA,&out);
  curl_easy_setopt(c,CURLOPT_TIMEOUT,30L);
  curl_easy_setopt(c,CURLOPT_SSL_VERIFYPEER,1L);
  curl_easy_setopt(c,CURLOPT_SSL_VERIFYHOST,2L);
  CURLcode rc=curl_easy_perform(c);
  if(code) curl_easy_getinfo(c,CURLINFO_RESPONSE_CODE,code);
  if(rc!=CURLE_OK) std::cerr<<"GET "<<url<<" err "<<curl_easy_strerror(rc)<<"\n";
  curl_easy_cleanup(c); return out;
}
static std::string http_post_json(const std::string& url,const std::string& body,long* code=nullptr){
  CURL* c=curl_easy_init(); std::string out; if(!c) return out;
  struct curl_slist* hdr=nullptr;
  hdr=curl_slist_append(hdr,"Content-Type: application/json");
  curl_easy_setopt(c,CURLOPT_URL,url.c_str());
  curl_easy_setopt(c,CURLOPT_HTTPHEADER,hdr);
  curl_easy_setopt(c,CURLOPT_POSTFIELDS,body.c_str());
  curl_easy_setopt(c,CURLOPT_WRITEFUNCTION,sink);
  curl_easy_setopt(c,CURLOPT_WRITEDATA,&out);
  curl_easy_setopt(c,CURLOPT_SSL_VERIFYPEER,1L);
  curl_easy_setopt(c,CURLOPT_SSL_VERIFYHOST,2L);
  CURLcode rc=curl_easy_perform(c);
  if(code) curl_easy_getinfo(c,CURLINFO_RESPONSE_CODE,code);
  if(rc!=CURLE_OK) std::cerr<<"POST "<<url<<" err "<<curl_easy_strerror(rc)<<"\n";
  curl_slist_free_all(hdr); curl_easy_cleanup(c); return out;
}
static long long now_ms(){
  return std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::system_clock::now().time_since_epoch()).count();
}

// ---------- Config ----------
static std::string getenv_s(const char* k){ const char* v=getenv(k); return v?std::string(v):std::string(); }
static fs::path exe_dir(){
  char buf[PATH_MAX]; ssize_t r=readlink("/proc/self/exe", buf, sizeof(buf)-1);
  if(r>0){ buf[r]=0; return fs::path(buf).parent_path(); }
  return fs::current_path();
}
static fs::path resolve_path(const fs::path& base, const std::string& comp){
  if(comp.empty()) return base;
  fs::path p(comp);
  return p.is_absolute()? p : (base / p);
}

struct Cfg {
  fs::path base_dir;     // local runtime (FIFO by default)
  fs::path data_dir;     // logs + state (can be NAS). If empty -> base_dir
  fs::path aliases_path;
  std::string worker;
  std::string phone_id;
  int lp_timeout_sec = 25;
  int pull_limit = 200;
  std::string fifo_name = "send.fifo";
  std::string fifo_path; // if set, overrides base_dir/fifo_name
  std::string global_log = "events.jsonl";
  std::string meta_log   = "meta.jsonl";
  std::string state_file = "state.json";
  std::string per_prefix = "events.";
  std::string per_suffix = ".jsonl";
};
static void merge_json(Cfg& c, const json& j){
  auto S=[&](const char* k,std::string& v){ if(j.contains(k)) v=j[k].get<std::string>(); };
  auto I=[&](const char* k,int& v){ if(j.contains(k)) v=j[k].get<int>(); };
  auto P=[&](const char* k,fs::path& v){ if(j.contains(k)) v=fs::path(j[k].get<std::string>()); };
  P("base_dir",c.base_dir); P("data_dir",c.data_dir); P("aliases_path",c.aliases_path);
  S("worker",c.worker); S("phone_id",c.phone_id);
  I("lp_timeout_sec",c.lp_timeout_sec); I("pull_limit",c.pull_limit);
  S("fifo_name",c.fifo_name); S("fifo_path",c.fifo_path);
  S("global_log",c.global_log); S("meta_log",c.meta_log); S("state_file",c.state_file);
  S("per_prefix",c.per_prefix); S("per_suffix",c.per_suffix);
}
static Cfg load_cfg(int argc, char** argv){
  Cfg c;
  fs::path home = getenv_s("HOME").empty()? fs::path(".") : fs::path(getenv_s("HOME"));
  c.base_dir = home / ".wa-hub";
  c.aliases_path = c.base_dir / "aliases.json";
  c.worker = getenv_s("WORKER");
  c.phone_id = getenv_s("WA_PHONE_ID");

  fs::path cfg_path;
  for(int i=1;i<argc;i++) if(std::string(argv[i])=="--config" && i+1<argc) { cfg_path=argv[i+1]; break; }
  if(cfg_path.empty()){ std::string envp=getenv_s("WA_HUB_CONFIG"); if(!envp.empty()) cfg_path=envp; }
  if(cfg_path.empty()) cfg_path = exe_dir() / "wa-hub.json";
  if(fs::exists(cfg_path)){ std::ifstream f(cfg_path); try{ json j; f>>j; merge_json(c,j);}catch(...){} }

  // env overrides
  std::string v;
  if(!(v=getenv_s("WA_HUB_BASE")).empty())    c.base_dir = v;
  if(!(v=getenv_s("WA_HUB_DATA")).empty())    c.data_dir = v;
  if(!(v=getenv_s("WA_HUB_ALIASES")).empty()) c.aliases_path = v;
  if(!(v=getenv_s("WA_HUB_FIFO")).empty())    c.fifo_path = v;
  if(!(v=getenv_s("WORKER")).empty())         c.worker = v;
  if(!(v=getenv_s("WA_PHONE_ID")).empty())    c.phone_id = v;

  // CLI overrides
  for(int i=1;i<argc;i++){
    std::string s=argv[i]; auto nxt=[&](std::string& out){ if(i+1<argc) out=argv[++i]; };
    if(s=="--base"){ std::string t; nxt(t); c.base_dir=t; }
    else if(s=="--data"){ std::string t; nxt(t); c.data_dir=t; }
    else if(s=="--aliases"){ std::string t; nxt(t); c.aliases_path=t; }
    else if(s=="--fifo"){ std::string t; nxt(t); c.fifo_path=t; }
    else if(s=="--worker"){ std::string t; nxt(t); c.worker=t; }
    else if(s=="--phone"){ std::string t; nxt(t); c.phone_id=t; }
    else if(s=="--timeout"){ std::string t; nxt(t); c.lp_timeout_sec=std::stoi(t); }
    else if(s=="--limit"){ std::string t; nxt(t); c.pull_limit=std::stoi(t); }
  }

  if(!c.worker.empty() && c.worker.back()=='/') c.worker.pop_back();
  if(c.data_dir.empty()) c.data_dir = c.base_dir;
  fs::create_directories(c.base_dir);
  fs::create_directories(c.data_dir);
  return c;
}

// ---------- Aliases ----------
struct Aliases{
  std::unordered_map<std::string,std::string> alias_to_num;
  std::unordered_map<std::string,std::string> num_to_alias;
};
static Aliases load_aliases(const fs::path& path){
  Aliases A; std::ifstream f(path); if(!f.good()) return A;
  json j; try{ f>>j; }catch(...){ return A; }
  auto add=[&](const std::string& a,const std::string& n){ A.alias_to_num[a]=n; A.num_to_alias[n]=a; };
  if(j.is_object()){
    if(j.contains("aliases") && j["aliases"].is_object()){
      for(auto it=j["aliases"].begin(); it!=j["aliases"].end(); ++it)
        if(it.value().is_string()) add(it.key(), it.value().get<std::string>());
    } else {
      for(auto it=j.begin(); it!=j.end(); ++it)
        if(it.value().is_string()) add(it.key(), it.value().get<std::string>());
    }
  }
  return A;
}
static std::string peer_key(const Aliases& A, const std::string& number){
  auto it=A.num_to_alias.find(number); return it==A.num_to_alias.end()? number : it->second;
}

// ---------- Per-contact logs ----------
class PerContactLogs{
  fs::path dir; std::mutex m; std::string pre,suf;
  std::unordered_map<std::string, std::unique_ptr<std::ofstream>> files;
  std::ofstream& stream_for(const std::string& key){
    auto it=files.find(key);
    if(it!=files.end()) return *(it->second);
    fs::create_directories(dir);
    auto p = dir/(pre+key+suf);
    auto ofs = std::make_unique<std::ofstream>(p, std::ios::app);
    files[key]=std::move(ofs);
    return *(files[key]);
  }
public:
  PerContactLogs(fs::path base, std::string prefix, std::string suffix)
    :dir(std::move(base)), pre(std::move(prefix)), suf(std::move(suffix)){ fs::create_directories(dir); }
  void append(const std::string& key, const json& line){
    std::lock_guard<std::mutex> lk(m);
    auto& s = stream_for(key);
    s<<line.dump()<<'\n';
    s.flush();
  }
};

// ---------- State ----------
static fs::path state_path(const Cfg& c){ return resolve_path(c.data_dir, c.state_file); }
static long long load_since_state(const Cfg& c){
  std::ifstream f(state_path(c)); if(!f.good()) return -1;
  json j; try{ f>>j; }catch(...){ return -1; }
  if(!j.is_object()) return -1;
  return j.value("since",-1LL);
}
static void save_since_state(const Cfg& c,long long since){
  fs::path p = state_path(c);
  fs::path tmp = p; tmp += ".tmp";
  json j={{"since",since},{"updated",now_ms()}};
  { std::ofstream o(tmp, std::ios::trunc); o<<j.dump(); o.flush(); }
  std::error_code ec; fs::rename(tmp, p, ec); if(ec) std::cerr<<"state rename err: "<<ec.message()<<"\n";
}

// ---------- Envelope processing ----------
static void process_envelope_and_log(const json& j, const Aliases& A,
                                     std::ofstream& global, PerContactLogs& pcl){
  if(!j.contains("messages") || !j["messages"].is_array()) return;
  for(const auto& b : j["messages"]){
    if(!b.contains("entry")||!b["entry"].is_array()) continue;
    for(const auto& e : b["entry"]){
      if(!e.contains("changes")||!e["changes"].is_array()) continue;
      for(const auto& ch : e["changes"]){
        if(!ch.contains("value")) continue;
        const auto& v = ch["value"];

        if(v.contains("messages") && v["messages"].is_array()){
          for(const auto& m : v["messages"]){
            if(m.value("type",std::string())=="text"){
              std::string from = m.value("from","");
              std::string text = m["text"].value("body","");
              std::string peer = peer_key(A, from);
              json ev = { {"ts", now_ms()}, {"kind","received"}, {"peer",peer}, {"text",text} };
              global<<ev.dump()<<'\n'; global.flush();
              pcl.append(peer, ev);
            }
          }
        }
        if(v.contains("statuses") && v["statuses"].is_array()){
          for(const auto& s : v["statuses"]){
            std::string to = s.value("recipient_id","");
            std::string peer = peer_key(A, to);
            std::string st = s.value("status","");
            json ev = { {"ts", now_ms()}, {"kind","status"}, {"peer",peer}, {"status",st} };
            global<<ev.dump()<<'\n'; global.flush();
            pcl.append(peer, ev);
          }
        }
      }
    }
  }
}

// ---------- Catch-up ----------
static long long catch_up_all_history(const Cfg& c, std::ofstream& global, PerContactLogs& pcl){
  long long cursor = 0;
  for(;;){
    long code=0;
    std::ostringstream url;
    url<<c.worker<<"/pull?since="<<cursor<<"&limit="<<c.pull_limit;
    auto body = http_get(url.str(), &code);
    if(code/100!=2){ std::cerr<<"pull http "<<code<<"\n"; break; }
    auto j = json::parse(body, nullptr, false);
    if(j.is_discarded()) break;

    Aliases A = load_aliases(c.aliases_path);
    process_envelope_and_log(j, A, global, pcl);

    long long next_since = j.value("next_since", cursor);
    long long count = j.value("count", 0LL);
    cursor = next_since;
    save_since_state(c, cursor);

    if(count == 0) break;
  }
  return cursor;
}

// ---------- MAIN ----------
int main(int argc,char**argv){
  // load config
  Cfg cfg = load_cfg(argc, argv);
  if(cfg.worker.empty() || cfg.phone_id.empty()){
    std::cerr<<"Set worker and phone_id via config/env/CLI\n";
    return 1;
  }

  // resolve paths
  fs::create_directories(cfg.base_dir);
  fs::create_directories(cfg.data_dir);
  fs::path fifo = cfg.fifo_path.empty()? (cfg.base_dir / cfg.fifo_name) : fs::path(cfg.fifo_path);
  fs::path glog = resolve_path(cfg.data_dir, cfg.global_log);
  fs::path mlog = resolve_path(cfg.data_dir, cfg.meta_log);

  if(!fs::exists(fifo)){ std::string cmd="mkfifo "+fifo.string(); std::system(cmd.c_str()); }

  // open FIFO with read end + keepalive writer so external writers never block
  int fd_r = ::open(fifo.c_str(), O_RDONLY | O_CLOEXEC);
  if(fd_r < 0){ std::perror("open fifo RDONLY"); return 2; }
  int fd_w_keepalive = ::open(fifo.c_str(), O_WRONLY | O_CLOEXEC);
  if(fd_w_keepalive < 0){ std::perror("open fifo WRONLY"); return 2; }
  FILE* fifo_in = fdopen(fd_r, "r");
  if(!fifo_in){ std::perror("fdopen fifo"); return 2; }

  std::ofstream global(glog, std::ios::app); if(!global.good()){ std::cerr<<"cannot open "<<glog<<"\n"; return 3; }
  std::ofstream meta(mlog, std::ios::app);   if(!meta.good()){ std::cerr<<"cannot open "<<mlog<<"\n"; return 4; }
  PerContactLogs pcl(cfg.data_dir, cfg.per_prefix, cfg.per_suffix);

  long long s0 = load_since_state(cfg);
  if(s0 < 0){ s0 = catch_up_all_history(cfg, global, pcl); }
  std::atomic<long long> since{s0};
  std::atomic<bool> running{true};

  // sender thread
  std::thread sender([&](){
    while(running){
      char* line=nullptr; size_t n=0; ssize_t r=getline(&line,&n,fifo_in);
      if(r<=0){ clearerr(fifo_in); std::this_thread::sleep_for(std::chrono::milliseconds(50)); continue; }
      std::string s(line, r); free(line);

      json cmd = json::parse(s, nullptr, false);
      if(cmd.is_discarded()){ std::cerr<<"bad send JSON: "<<s<<"\n"; continue; }

      Aliases A = load_aliases(cfg.aliases_path);
      std::string to = cmd.value("to","");
      if(to.empty() && cmd.contains("alias")) to = cmd.value("alias","");
      std::string text = cmd.value("text","");
      if(to.empty()||text.empty()){ std::cerr<<"send needs {to|alias, text}\n"; continue; }
      if(A.alias_to_num.count(to)) to = A.alias_to_num[to];

      json payload = {{"phone_number_id",cfg.phone_id},{"to",to},{"text",text}};
      long code=0; auto resp=http_post_json(cfg.worker+"/send", payload.dump(), &code);
      long long ts = now_ms();
      std::string peer = peer_key(A, to);

      // meta debug
      json jr = json::parse(resp, nullptr, false);
      json meta_line = {{"ts",ts},{"op","send"},{"http",code},{"to",to},{"text",text},{"phone_number_id",cfg.phone_id}};
      if(code/100==2){
        json ok;
        if(!jr.is_discarded()){
          if(jr.contains("contacts") && jr["contacts"].is_array() && !jr["contacts"].empty())
            ok["wa_id"] = jr["contacts"][0].value("wa_id","");
          if(jr.contains("messages") && jr["messages"].is_array() && !jr["messages"].empty())
            ok["message_id"] = jr["messages"][0].value("id","");
        }
        meta_line["meta"]=ok;
      } else {
        json err;
        if(!jr.is_discarded() && jr.contains("error")){
          const auto& e = jr["error"];
          err["code"]=e.value("code",0);
          err["type"]=e.value("type",std::string());
          err["message"]=e.value("message",std::string());
          if(e.contains("error_data")) err["details"]=e["error_data"].value("details",std::string());
          err["fbtrace_id"]=e.value("fbtrace_id",std::string());
        } else { err["message"]="non-JSON or empty response"; err["raw"]=resp; }
        meta_line["error"]=err;
      }
      meta<<meta_line.dump()<<'\n'; meta.flush();

      // normal logs
      if(code/100==2){
        json ev = { {"ts",ts},{"kind","sent"},{"peer",peer},{"text",text} };
        global<<ev.dump()<<'\n'; global.flush();
        pcl.append(peer, ev);
      } else {
        json ev = { {"ts",ts},{"kind","status"},{"peer",peer},{"status","failed"} };
        global<<ev.dump()<<'\n'; global.flush();
        pcl.append(peer, ev);
      }
    }
  });

  // receiver loop
  while(running){
    long code=0;
    std::ostringstream url;
    url<<cfg.worker<<"/lp?since="<<since.load()<<"&timeout="<<cfg.lp_timeout_sec
       <<"&limit="<<cfg.pull_limit;
    auto body = http_get(url.str(), &code);
    if(code/100!=2){ std::cerr<<"lp http "<<code<<"\n"; std::this_thread::sleep_for(std::chrono::milliseconds(250)); continue; }

    auto j = json::parse(body, nullptr, false);
    if(j.is_discarded()) continue;
    long long next_since = j.value("next_since", since.load());

    Aliases A = load_aliases(cfg.aliases_path);
    process_envelope_and_log(j, A, global, pcl);

    since.store(next_since);
    save_since_state(cfg, next_since);
  }
  return 0;
}
