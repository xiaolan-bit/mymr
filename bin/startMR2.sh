#!/bin/bash

echo "Starting driver..."
nohup java -jar mymr-1.0.jar > driver.log 2>&1 &

# 等待 3 秒
echo "Waiting for 3 seconds..."
sleep 3

# 启动第二个服务
echo "Starting executors..."
#java -cp mymr-1.0.jar com.ksc.wordcount.worker.UrlTopNExecutor > executors.log 2>&1 &
# 获取当前脚本所在文件夹目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 读取slave.conf文件，提取每行的第一个IP地址并进行去重
unique_ips=()
while IFS= read -r line; do
    # 去除首尾的空白字符
    line=$(echo "$line" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')

    # 跳过空行和以#开头的注释行
    if [[ -z "$line" || "$line" =~ ^\# ]]; then
        continue
    fi

    ip=$(echo "$line" | awk '{print $1}')
    unique_ips+=("$ip")
done < "slave.conf"

# 去重操作
unique_ips=($(echo "${unique_ips[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' '))

# 遍历去重后的IP列表，通过SSH连接到IP并执行命令
remote="java -cp mymr-1.0.jar com.ksc.wordcount.worker.UrlTopNExecutor"
for ip in "${unique_ips[@]}"; do
    echo "Starting executor on $ip..."
    ssh "root@$ip" "cd '$SCRIPT_DIR' && java -cp mymr-1.0.jar com.ksc.wordcount.worker.UrlTopNExecutor > executor.log 2>&1 &"
done

echo "All actions are completed."