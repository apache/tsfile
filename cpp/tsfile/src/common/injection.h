#ifndef COMMON_INJECTION_H
#define COMMON_INJECTION_H

#include <iostream>
#include <string>
#include <map>

namespace timecho
{
namespace common
{

// define struct 
struct InjectPoint
{
  int count_down_; // left count
};

// define DBUG_EXECUTE_IF
#define DBUG_EXECUTE_IF(inject_point_name, code) \
  do { \
    common::MutexGuard mg(g_all_inject_points_mutex); \
    if (g_all_inject_points.find(inject_point_name) != g_all_inject_points.end()) { \
      InjectPoint& inject_point = g_all_inject_points[inject_point_name]; \
      if (inject_point.count_down_ <= 0) { \
        code; \
      } \
      inject_point.count_down_--; \
    } \
  } while(0)

// open injection
#define ENABLE_INJECTION(inject_point_name, count) \
  do { \
    common::MutexGuard mg(g_all_inject_points_mutex); \
    g_all_inject_points[inject_point_name] = {count}; \
  } while(0)

// close injection 
#define DISABLE_INJECTION(inject_point_name) \
  do { \
    common::MutexGuard mg(g_all_inject_points_mutex); \
    g_all_inject_points.erase(inject_point_name); \
  } while(0)

// the map save all inject points
extern Mutex g_all_inject_points_mutex;
extern std::map<std::string, InjectPoint> g_all_inject_points;

} // end namespace common
} // end namespace timecho

#endif // COMMON_INJECTION_H
