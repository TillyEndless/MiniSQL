#include "buffer/clock_replacer.h"
#include "glog/logging.h"
#include <iostream>

CLOCKReplacer::CLOCKReplacer(std::size_t num_pages):capacity(num_pages) {
    clock_list.clear();
    pointer = clock_list.end();
    clock_status.clear();
}
   
CLOCKReplacer::~CLOCKReplacer() {
    clock_list.clear();
    clock_status.clear();
}   

bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
    std::lock_guard<std::recursive_mutex> guard(latch_);
    if(clock_list.empty()) return false;

    if(pointer == clock_list.end()) pointer = clock_list.begin();
    while (1) {
        if(clock_list.empty()){
            // LOG(ERROR) << "[clock replacer] clock becomes empty ???"; //不会用，空着吧
            std::cerr << "[clock replacer] clock becomes empty ???" << std::endl;
        }
        frame_id_t fid = (*pointer);
        if(clock_status[fid] != 0) {
            clock_status[fid] = 0;
            ++pointer;
            if(pointer == clock_list.end()) pointer = clock_list.begin();
        } else {
            *frame_id = fid;
            auto it = pointer++;
            clock_list.erase(it);
            clock_status.erase(fid);
            return true;
        }
    }
}
   
void CLOCKReplacer::Pin(frame_id_t frame_id) {
    std::lock_guard<std::recursive_mutex> guard(latch_);
    auto it = std::find(clock_list.begin(),clock_list.end(),frame_id);
    if(it != clock_list.end()) {
        if(it == pointer) pointer++;
        clock_list.erase(it);
        clock_status.erase(frame_id);
        if(pointer == clock_list.end()) pointer = clock_list.begin();
    }
}
   
void CLOCKReplacer::Unpin(frame_id_t frame_id) {
    std::lock_guard<std::recursive_mutex> guard(latch_);

    if (clock_status.find(frame_id) != clock_status.end() || clock_list.size() >= capacity) return ;
    clock_list.push_back(frame_id);
    clock_status[frame_id] = true;
    if (pointer == clock_list.end()) pointer = clock_list.begin();
}
   
std::size_t CLOCKReplacer::Size() {
    std::lock_guard<std::recursive_mutex> guard(latch_);
    return clock_list.size();
}