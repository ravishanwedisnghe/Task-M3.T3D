// Traffic simulator: C++17, blocking bounded buffer, hourly totals.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
using Summary = std::map<std::string, std::map<std::string, unsigned long long>>;
using Batch = std::vector<std::string>;
// Keep all lights ordered, so a light outside top N can later enter it.
struct CongestionOrder {
    bool operator()(const std::pair<std::string,unsigned long long>& a,
                    const std::pair<std::string,unsigned long long>& b) const {
        return a.second != b.second ? a.second > b.second : a.first < b.first;
    }
};
using OrderedLights = std::set<std::pair<std::string,unsigned long long>, CongestionOrder>;
using OrderedHours = std::map<std::string, OrderedLights>;

// Queue capacity is measured in records, not batches.
class Buffer {
    std::deque<Batch> queue;
    size_t capacity, records = 0;
    bool closed = false;
    std::mutex mutex;
    std::condition_variable notEmpty, notFull;
public:
    explicit Buffer(size_t n): capacity(n) {}
    void push(Batch batch) {
        std::unique_lock<std::mutex> lock(mutex);
        notFull.wait(lock, [&]{ return records + batch.size() <= capacity; });
        records += batch.size();
        queue.push_back(std::move(batch));
        lock.unlock(); notEmpty.notify_one();
    }
    bool pop(Batch& batch) {
        std::unique_lock<std::mutex> lock(mutex);
        notEmpty.wait(lock, [&]{ return closed || !queue.empty(); });
        if (queue.empty()) return false;
        batch = std::move(queue.front()); queue.pop_front();
        records -= batch.size();
        lock.unlock(); notFull.notify_all();
        return true;
    }
    void close() {
        { std::lock_guard<std::mutex> lock(mutex); closed = true; }
        notEmpty.notify_all();
    }
};

// Generated CSV format is strict: YYYY-MM-DD HH:MM:SS,TLid,nonnegative count.
void process(const std::string& line, Summary& totals, OrderedHours& ordered) {
    auto a = line.find(',');
    auto b = a == std::string::npos ? a : line.find(',', a + 1);
    if (a != 19 || b == std::string::npos || b == a+1 || line.find(',',b+1)!=std::string::npos)
        throw std::runtime_error("Invalid CSV record: " + line);
    auto stamp = line.substr(0,a);
    if(stamp[4]!='-' || stamp[7]!='-' || stamp[10]!=' ' || stamp[13]!=':' || stamp[16]!=':')
        throw std::runtime_error("Invalid timestamp: " + stamp);
    for(size_t i=0;i<stamp.size();++i)
        if(i!=4 && i!=7 && i!=10 && i!=13 && i!=16 && (stamp[i]<'0'||stamp[i]>'9'))
            throw std::runtime_error("Invalid timestamp: " + stamp);
    if(std::stoi(stamp.substr(11,2))>23 || std::stoi(stamp.substr(14,2))>59 || std::stoi(stamp.substr(17,2))>59)
        throw std::runtime_error("Invalid time: " + stamp);
    auto count = line.substr(b+1);
    if(count.empty() || count.find_first_not_of("0123456789")!=std::string::npos)
        throw std::runtime_error("Invalid car count: " + line);
    auto hour = stamp.substr(0,13)+":00";
    auto id = line.substr(a+1,b-a-1);
    auto cars = std::stoull(count);
    auto& total = totals[hour][id];
    auto& ranking = ordered[hour];
    ranking.erase({id,total});
    total += cars;
    ranking.insert({id,total}); // Ordered after every measurement, not only at EOF.

}
size_t positive(const char* text) {
    std::string s(text);
    if(s.empty() || s.find_first_not_of("0123456789")!=std::string::npos)
        throw std::runtime_error("Settings must be positive integers");
    auto n=std::stoull(s); if(!n) throw std::runtime_error("Settings must be positive"); return n;
}
int main(int argc,char** argv) {
    try {
        if(argc!=4 && argc!=7) {
            std::cerr << "Sequential: ./traffic_sim seq FILE TOP_N\nParallel: ./traffic_sim par FILE TOP_N PRODUCERS CONSUMERS BUFFER_RECORDS\n";
            return 1;
        }
        std::string mode=argv[1], filename=argv[2];
        if((mode!="seq" && mode!="par") || (mode=="seq" && argc!=4) || (mode=="par" && argc!=7))
            throw std::runtime_error("Invalid mode or argument count");
        size_t top=positive(argv[3]), p=mode=="par"?positive(argv[4]):0,
            c=mode=="par"?positive(argv[5]):0, cap=mode=="par"?positive(argv[6]):0;
        if(p>128 || c>128) throw std::runtime_error("Use at most 128 producers and 128 consumers");
        std::cout<<"Mode: "<<mode<<"\nInput file: "<<filename<<"\nTop N: "<<top
                 <<"\nProducers: "<<p<<" Consumers: "<<c<<" Buffer records: "<<cap
                 <<"\nHardware threads: "<<std::thread::hardware_concurrency()<<std::endl;
        auto start=std::chrono::steady_clock::now();
        std::ifstream input(filename);
        if(!input) throw std::runtime_error("Cannot open input file");
        std::string header; std::getline(input,header);
        if(!header.empty() && header.back()=='\r') header.pop_back();
        if(header!="timestamp,traffic_light_id,cars") throw std::runtime_error("CSV header is missing or incorrect");
        Summary result; OrderedHours orderedResult; unsigned long long processed=0;
        if(mode=="seq") {
            std::string line;
            while(std::getline(input,line)) {
                if(!line.empty() && line.back()=='\r') line.pop_back();
                process(line,result,orderedResult); ++processed;
            }
        } else {
            Buffer buffer(cap);
            std::mutex fileMutex, errorMutex;
            std::atomic<size_t> active(p);
            std::exception_ptr error;
            auto saveError=[&]{ std::lock_guard<std::mutex> lock(errorMutex); if(!error) error=std::current_exception(); };
            std::vector<Summary> local(c);
            std::vector<OrderedHours> localOrdered(c);
            std::vector<unsigned long long> counts(c,0);
            std::vector<std::thread> producers,consumers;
            // Private consumer summaries need no locks; merge only after joining.
            for(size_t i=0;i<c;++i) consumers.emplace_back([&,i]{
                Batch batch; bool failed=false;
                while(buffer.pop(batch)) {
                    if(failed) continue; // Drain on invalid input so producers never deadlock.
                    try { for(auto& line:batch) { process(line,local[i],localOrdered[i]); ++counts[i]; } }
                    catch(...) { saveError(); failed=true; }
                }
            });
            for(size_t i=0;i<p;++i) producers.emplace_back([&]{
                try {
                    while(true) {
                        Batch batch;
                        { std::lock_guard<std::mutex> lock(fileMutex);
                          std::string line;
                          for(size_t j=0;j<std::min<size_t>(256,cap) && std::getline(input,line);++j) {
                              if(!line.empty() && line.back()=='\r') line.pop_back();
                              batch.push_back(std::move(line));
                          }
                        }
                        if(batch.empty()) break;
                        buffer.push(std::move(batch));
                    }
                } catch(...) { saveError(); }
                if(active.fetch_sub(1)==1) buffer.close();
            });
            for(auto& t:producers) t.join();
            for(auto& t:consumers) t.join();
            if(error) std::rethrow_exception(error);
            for(size_t i=0;i<c;++i) {
                processed+=counts[i];
                // Consume each worker's ordered list. Merge every light, not only its top N:
                // a global winner might be below top N in an individual worker.
                for(auto& hour:localOrdered[i]) for(auto& light:hour.second)
                    result[hour.first][light.first]+=light.second;
            }
        }
        if(input.bad()) throw std::runtime_error("Input read failed");
        // Sorting is included in timing; console output is excluded in both modes.
        using Ranking=std::vector<std::pair<std::string,unsigned long long>>;
        std::map<std::string,Ranking> rankings;
        for(auto& hour:result) {
            Ranking sorted(hour.second.begin(),hour.second.end());
            std::sort(sorted.begin(),sorted.end(),[](auto& a,auto& b){return a.second!=b.second?a.second>b.second:a.first<b.first;});
            if(sorted.size()>top) sorted.resize(top);
            rankings[hour.first]=std::move(sorted);
        }
        double seconds=std::chrono::duration<double>(std::chrono::steady_clock::now()-start).count();
        for(auto& hour:rankings) {
            std::cout<<"Hour: "<<hour.first<<'\n'; size_t rank=0;
            for(auto& light:hour.second) std::cout<<"Rank "<<++rank<<": "<<light.first<<" - "<<light.second<<" cars\n";
        }
        std::cout<<"Records processed: "<<processed<<"\nHours processed: "<<result.size()
                 <<"\nExecution time: "<<std::fixed<<std::setprecision(6)<<seconds<<" seconds\n";
    } catch(const std::exception& e) { std::cerr<<"Error: "<<e.what()<<'\n'; return 1; }
}
