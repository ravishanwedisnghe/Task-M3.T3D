#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <string>
#include <stdexcept>
unsigned long long positive(const char* value) {
    std::string s(value);
    if(s.empty() || s.find_first_not_of("0123456789")!=std::string::npos) throw std::runtime_error("Use positive integer settings");
    auto n=std::stoull(s); if(!n) throw std::runtime_error("Settings must be positive"); return n;
}
int main(int argc,char** argv) {
    try {
        if(argc!=4) {std::cerr<<"Usage: ./generate_data FILE LIGHTS HOURS\n";return 1;}
        auto lights=positive(argv[2]), hours=positive(argv[3]);
        if(hours>8760 || lights>1000000) throw std::runtime_error("Limits: 8760 hours and 1000000 lights");
        std::ofstream out(argv[1]); if(!out) throw std::runtime_error("Cannot create file");
        out<<"timestamp,traffic_light_id,cars\n";
        // Start 2026-01-01; month lengths keep dates valid for the whole year.
        int month=1,day=1; int days[]={31,28,31,30,31,30,31,31,30,31,30,31};
        for(unsigned long long h=0;h<hours;++h) {
            for(int slot=0;slot<12;++slot) for(unsigned long long light=1;light<=lights;++light) {
                auto cars=10+(light*17+h*13+slot*7)%91;
                out<<"2026-"<<std::setfill('0')<<std::setw(2)<<month<<'-'<<std::setw(2)<<day<<' '
                   <<std::setw(2)<<h%24<<':'<<std::setw(2)<<slot*5<<":00,TL"<<std::setw(6)<<light<<','<<cars<<'\n';
            }
            if(h%24==23) {if(++day>days[month-1]) {day=1;++month;}}
        }
        out.close(); if(!out) throw std::runtime_error("Writing failed; check free disk space");
        std::cout<<"Input file: "<<argv[1]<<"\nLights: "<<lights<<"\nHours: "<<hours
                 <<"\nMeasurements per light per hour: 12\nRecords: "<<lights*hours*12<<'\n';
    }catch(const std::exception& e){std::cerr<<"Error: "<<e.what()<<'\n';return 1;}
}
