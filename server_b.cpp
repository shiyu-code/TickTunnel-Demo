/*
 * 中间层 B 
 * 1. TCP 客户端全局静态，io_context 永不空跑
 * 2. 无锁队列 + 后台聚合线程
 * 3. WebSocket 推送 + SQLite 落库
 */
#include <websocketpp/config/asio_no_tls.hpp>
#include <websocketpp/server.hpp>
#include <boost/asio.hpp>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <sqlite3.h>
#include <nlohmann/json.hpp>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <mutex>
#include <map>
#include <vector>
#include <thread>
#include <chrono>
#include <fstream>
#include <atomic>
#include <queue>
#include <functional>
#include <set>

namespace asio = boost::asio;
using tcp = asio::ip::tcp;
using error_code = boost::system::error_code;
using wsserver = websocketpp::server<websocketpp::config::asio>;
using json = nlohmann::json;
using con_hdl = websocketpp::connection_hdl;

const std::string SECRET = "exchange-a-secret";
const std::string DB_FILE = "tick_1min.db";

// ---------- 无锁队列 ----------
template<typename T>
class ring_queue {
public:
    explicit ring_queue(std::size_t n) : buf_(n), head_(0), tail_(0) {}
    bool push(const T& v) {
        auto t = tail_.load(std::memory_order_relaxed);
        auto next = (t + 1) % buf_.size();
        if (next == head_.load(std::memory_order_acquire)) return false;
        buf_[t] = v;
        tail_.store(next, std::memory_order_release);
        return true;
    }
    bool pop(T& v) {
        auto h = head_.load(std::memory_order_relaxed);
        if (h == tail_.load(std::memory_order_acquire)) return false;
        v = std::move(buf_[h]);
        head_.store((h + 1) % buf_.size(), std::memory_order_release);
        return true;
    }
private:
    std::vector<T> buf_;
    std::atomic<std::size_t> head_{ 0 }, tail_{ 0 };
};

// ---------- 全局 ----------
sqlite3* g_db = nullptr;
wsserver g_wss;
std::set<con_hdl, std::owner_less<con_hdl>> g_cons;
std::mutex g_cons_mtx;
ring_queue<json> g_q(65536);

// ---------- 工具 ----------
static std::string hmac_sha256(const std::string& msg)
{
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int len = 0;
    HMAC(EVP_sha256(), SECRET.data(), static_cast<int>(SECRET.size()),
        reinterpret_cast<const unsigned char*>(msg.data()), msg.size(),
        digest, &len);
    std::stringstream ss;
    for (unsigned int i = 0; i < len; ++i)
        ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(digest[i]);
    return ss.str();
}

// ---------- 广播 ----------
void broadcast(const json& j)
{
    std::string payload = j.dump();
    std::lock_guard<std::mutex> lk(g_cons_mtx);
    auto con_list = g_cons;
    for (auto hdl : con_list)
    {
        try
        {
            auto con = g_wss.get_con_from_hdl(hdl);
            if (con) g_wss.send(hdl, payload, websocketpp::frame::opcode::text);
        }
        catch (...) { /* ignore */ }
    }
}

// ---------- 后台聚合 ----------
void consumer_thread()
{
    std::map<std::string, json> acc;
    int64_t last_sec = 0;
    json j;
    while (true)
    {
        if (g_q.pop(j))
        {
            int64_t ts = j.contains("ts") ? j["ts"].get<int64_t>() : j.value("timestamp", int64_t(0));
            const std::string& sym = j["symbol"];
            if (ts != last_sec && last_sec != 0)
            {
                for (auto it = acc.begin(); it != acc.end(); ++it)
                {
                    const std::string& s = it->first;
                    json& bar = it->second;
                    if (bar.is_null() || !bar.contains("open")) continue;
                    try
                    {
                        char sql[512];
                        snprintf(sql, sizeof(sql),
                            "INSERT INTO tick_1min(symbol,ts,open,high,low,close,volume) "
                            "VALUES('%s',%lld,%.5f,%.5f,%.5f,%.5f,%lld);",
                            s.c_str(), static_cast<long long>(last_sec),
                            bar["open"].get<double>(), bar["high"].get<double>(),
                            bar["low"].get<double>(), bar["close"].get<double>(),
                            static_cast<long long>(bar["volume"].get<int64_t>()));
                        char* err = nullptr;
                        sqlite3_exec(g_db, sql, nullptr, nullptr, &err);
                        if (err) { std::cerr << "SQL err:" << err << '\n'; sqlite3_free(err); }
                        broadcast(bar);
                    }
                    catch (const std::exception& e)
                    {
                        std::cerr << "broadcast exception: " << e.what() << '\n';
                    }
                }
                acc.clear();
            }
            auto& bar = acc[sym];
            if (bar.empty())
                bar = { {"symbol",sym}, {"ts",ts}, {"open",j["close"]}, {"high",j["high"]}, {"low",j["low"]}, {"close",j["close"]}, {"volume",j["volume"]} };
            else
            {
                bar["high"] = std::max(bar["high"].get<double>(), j["high"].get<double>());
                bar["low"] = std::min(bar["low"].get<double>(), j["low"].get<double>());
                bar["close"] = j["close"];
                bar["volume"] = bar["volume"].get<int64_t>() + j["volume"].get<int64_t>();
            }
            last_sec = ts;
        }
        else
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

// ---------- TCP 客户端（全局静态，永不析构） ----------
class tcp_client
{
public:
    tcp_client(asio::io_context& ioc, const std::string& host, const std::string& port)
        : resolver_(ioc), socket_(ioc), host_(host), port_(port)
    {
        do_resolve();
    }

private:
    void do_resolve()
    {
        resolver_.async_resolve(host_, port_,
            [this](error_code ec, tcp::resolver::results_type eps) {
                if (!ec) { endpoints_ = eps; do_connect(); }
                else { reconnect(); }
            });
    }

    void do_connect()
    {
        asio::async_connect(socket_, endpoints_,
            [this](error_code ec, tcp::endpoint ep) {
                if (!ec)
                {
                    std::cout << "[TCP] connected -> " << ep << "\n";
                    do_read();
                }
                else
                {
                    std::cerr << "[TCP] connect failed: " << ec.message() << "\n";
                    reconnect();
                }
            });
    }

    void reconnect()
    {
        socket_.close();
        std::this_thread::sleep_for(std::chrono::seconds(1));
        do_resolve();
    }

    void do_read()
    {
        asio::async_read_until(socket_, buf_, '\n',
            [this](error_code ec, std::size_t n) {
                if (!ec)
                {
                    // 使用 istream 精确提取单行，避免缓冲边界问题影响签名
                    std::istream is(&buf_);
                    std::string line;
                    std::getline(is, line);
                    buf_.consume(n);
                    on_line(line);
                    do_read();
                }
                else
                {
                    std::cerr << "[TCP] read failed: " << ec.message() << "\n";
                    reconnect();
                }
            });
    }

    void on_line(const std::string& line)
    {
        size_t pos = line.rfind("|sig:");
        if (pos == std::string::npos) return;
        std::string body = line.substr(0, pos);
        std::string sig = line.substr(pos + 5);
        // 去除可能的尾部空白字符
        while (!sig.empty() && (sig.back() == '\r' || sig.back() == '\n' || sig.back() == ' ')) sig.pop_back();
        if (hmac_sha256(body) != sig) { std::cerr << "[TCP] bad sig\n"; return; }
        json j = json::parse(body);
        // 字段兼容：把 timestamp 映射为 ts 供聚合使用
        if (j.contains("timestamp") && !j.contains("ts")) j["ts"] = j["timestamp"];
        if (!g_q.push(j)) std::cerr << "[TCP] queue full, drop frame\n";
    }

    tcp::resolver resolver_;
    tcp::socket socket_;
    asio::streambuf buf_;
    std::string host_, port_;
    tcp::resolver::results_type endpoints_;
};

// ---------- WebSocket HTTP ----------
void http_handler(con_hdl hdl)
{
    auto con = g_wss.get_con_from_hdl(hdl);
    if (con->get_resource() == "/")
    {
        std::ifstream f("web/index.html");
        if (!f) { con->set_status(websocketpp::http::status_code::not_found); return; }
        std::ostringstream oss;
        oss << f.rdbuf();
        con->set_body(oss.str());
        con->set_status(websocketpp::http::status_code::ok);
    }
    else if (con->get_resource() == "/ws")
    {
        // auto upgrade
    }
    else con->set_status(websocketpp::http::status_code::not_found);
}

// ---------- 全局静态客户端 ----------
static tcp_client* g_client = nullptr;

int main()
{
    try
    {
        // 1. DB
        if (sqlite3_open(DB_FILE.c_str(), &g_db) != SQLITE_OK)
        {
            std::cerr << "can't open db: " << sqlite3_errmsg(g_db) << '\n';
            return 1;
        }
        sqlite3_exec(g_db, "CREATE TABLE IF NOT EXISTS tick_1min(symbol TEXT, ts INT, open REAL, high REAL, low REAL, close REAL, volume INT);", nullptr, nullptr, nullptr);

        // 2. WebSocket
        g_wss.init_asio();
        g_wss.set_open_handler([](con_hdl h) { std::lock_guard<std::mutex> lk(g_cons_mtx); g_cons.insert(h); });
        g_wss.set_close_handler([](con_hdl h) { std::lock_guard<std::mutex> lk(g_cons_mtx); g_cons.erase(h);   });
        g_wss.set_http_handler(http_handler);
        g_wss.listen(9002);
        g_wss.start_accept();

        // 3. TCP 客户端（全局单例，永不析构）
        asio::io_context ioc;
        asio::io_context::work guard(ioc);   // 防止 io_context 空跑
        g_client = new tcp_client(ioc, "127.0.0.1", "9001");

        std::thread ioc_thr([&] { ioc.run(); });
        std::thread ws_thr([&] { g_wss.run(); });
        std::thread cons_thr(consumer_thread);

        std::cout << "B 已启动，按回车退出...\n";
        std::cin.get();

        ioc.stop();
        g_wss.stop();
        ioc_thr.join();
        ws_thr.join();
        cons_thr.join();
    }
    catch (const std::exception& e)
    {
        std::cerr << "*** TOP LEVEL EXCEPTION: " << e.what() << '\n';
        std::cin.get();
        return 1;
    }
    catch (...)
    {
        std::cerr << "*** UNKNOWN EXCEPTION\n";
        std::cin.get();
        return 2;
    }
}