# OpenCCTV/mc

Memcached/repcached metrics for monitoring. 

Memcached/repcached 监控数据采集。

基于版本修改 https://github.com/iambocai/falcon-monit-scripts/blob/master/memcached/memcached-monitor.py 

改动：
 - 去掉多处硬编码
  - 灵活兼容不同操作系统平台 Linux、macOS
  - 自定义 open-falcon agent PUSH URL
  - 支持匹配自定义进程名 repcached
 - 增加 TCP/HTTP 超时控制
 - 仅支持 Python3 
 - 完善 logging


