#include <iostream>

#ifdef DEBUG_LOG
#define LOG(X) std::cout << X << std::endl
#else
#define LOG(X) ((void)0)
#endif

#ifdef DEBUG_ERROR
#define LOGE(X) std::cout << X << std::endl
#else
#define LOGE(X) ((void)0)
#endif

#ifdef DEBUG_WARN
#define LOGW(X) std::cout << X << std::endl
#else
#define LOGW(X) ((void)0)
#endif
