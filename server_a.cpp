/*
 * 交易所 A 
 * 1. 每条帧独立 async_write，session 内串行
 * 2. 帧格式：{"..."}|sig:xxx\n   单帧单 \n
 * 3. 高水位丢帧，但不粘包
 */
#include <boost/asio.hpp>
#include <iostream>
#include <iomanip>
#include <random>
#include <sstream>
#include <queue>
#include <atomic>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <unordered_set>

namespace asio = boost::asio;
using tcp = asio::ip::tcp;
using error_code = boost::system::error_code;
using std::this_thread::sleep_for;
using namespace std::chrono;

const std::string SECRET = "exchange-a-secret";

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

// ---------- session：全部 public ----------
class session : public std::enable_shared_from_this<session>
{
public:
    tcp::socket socket_;
    asio::streambuf read_buf_;
    std::queue<std::string> q_;
    asio::strand<asio::any_io_executor> strand_;
    std::atomic<bool> writing_{ false };

    explicit session(tcp::socket s) : socket_(std::move(s)), strand_(socket_.get_executor()) {}

    void start() { do_read(); }

    void deliver(std::string frame)
    {
        asio::post(strand_, [self = shared_from_this(), frame = std::move(frame)]() mutable {
            self->do_deliver(std::move(frame));
        });
    }

    void do_deliver(std::string frame)
    {
        frame += '\n';
        bool was_idle = writing_.exchange(true, std::memory_order_acquire) == false;
        q_.push(std::move(frame));
        if (was_idle) do_write();
    }

    void do_read()
    {
        asio::async_read_until(socket_, read_buf_, '\n',
            [self = shared_from_this()](error_code ec, std::size_t) {
                if (!ec) self->do_read();
            });
    }

    void do_write()
    {
        if (q_.empty()) { writing_.store(false, std::memory_order_release); return; }
        asio::async_write(socket_, asio::buffer(q_.front()),
            asio::bind_executor(strand_,
                [self = shared_from_this()](error_code ec, std::size_t) {
                    if (!ec)
                    {
                        self->q_.pop();
                        self->do_write();
                    }
                    else
                    {
                        std::cerr << "async_write failed: " << ec.message() << '\n';
                        self->socket_.close();
                        self->writing_.store(false, std::memory_order_release);
                    }
                }));
    }
};

// ---------- server：全部 public ----------
class server
{
public:
    asio::io_context ioc_;
    tcp::acceptor acc_;
    std::vector<std::shared_ptr<session>> sessions_;
    std::mutex mtx_;

    server() : acc_(ioc_, { tcp::v4(), 9001 }) {}

    void run()
    {
        std::cout << "Exchange-A listen on 9001\n";
        do_accept();
        ioc_.run();
    }

    void do_accept()
    {
        acc_.async_accept(
            [this](error_code ec, tcp::socket s) {
                if (!ec)
                {
                    std::lock_guard<std::mutex> lk(mtx_);
                    sessions_.erase(
                        std::remove_if(sessions_.begin(), sessions_.end(),
                            [](auto& sp) { return !sp->socket_.is_open(); }),
                        sessions_.end());
                    auto sess = std::make_shared<session>(std::move(s));
                    sess->start();
                    sessions_.push_back(sess);
                }
                do_accept();
            });
    }
};

// ---------- 生成 + 广播 ----------
void generate_thread(server* srv)
{
    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<> cnt(100, 300);
    std::uniform_int_distribution<> pick_idx(0, 999);
    std::uniform_real_distribution<> price_noise(-0.00030, 0.00030);
    std::uniform_int_distribution<> vol(1000, 5000);

    while (true)
    {
        int64_t now_s = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
        int pick = cnt(rng);
        std::vector<std::string> batch;
        batch.reserve(pick * 2);

        // 随机选择唯一的品种索引
        std::unordered_set<int> sel;
        while ((int)sel.size() < pick) sel.insert(pick_idx(rng));

        for (int idx : sel)
        {
            std::ostringstream symos;
            symos << 'S' << std::setw(4) << std::setfill('0') << (idx + 1);
            std::string sym = symos.str();
            double base = 1.17000 + (idx % 100) * 0.00010;
            for (int j = 0; j < 2; ++j)
            {
                double close = base + price_noise(rng);
                double high = close + std::abs(price_noise(rng));
                double low = close - std::abs(price_noise(rng));
                int64_t v = vol(rng);

                std::ostringstream os;
                os << std::fixed << std::setprecision(5);
                os << "{\"symbol\":\"" << sym << "\","
                    << "\"open\":" << base << ","
                    << "\"high\":" << high << ","
                    << "\"low\":" << low << ","
                    << "\"close\":" << close << ","
                    << "\"volume\":" << v << ","
                    << "\"ts\":" << now_s << "}";
                std::string body = os.str();
                std::string sig = hmac_sha256(body);
                batch.emplace_back(body + "|sig:" + sig);   // 无 \n
            }
        }
        std::shuffle(batch.begin(), batch.end(), rng);

        /* 逐条广播：每条独立 deliver → 每帧独立 async_write */
        std::lock_guard<std::mutex> lk(srv->mtx_);
        for (auto& frame : batch)
        {
            for (auto& s : srv->sessions_)
                s->deliver(frame);   // 每个 session 内部顺序写
        }

        sleep_for(1s);
    }
}

int main()
{
    server srv;
    std::thread prod([&srv] { generate_thread(&srv); });
    srv.run();
    prod.join();
    return 0;
}