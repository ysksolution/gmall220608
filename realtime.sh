#!/bin/bash

flink=/opt/module/flink-1.13.6/bin/flink
jar=/opt/gmall220608/gmall-realtime-1.0-SNAPSHOT.jar

# 存储的要运行的主类的全类名
apps=(
com.atguigu.gmall.realtime.app.dim.DimApp
com.atguigu.gmall.realtime.app.dwd.log.Dwd_01_DwdBaseLogApp
)

running_apps=`$flink list 2>/dev/null | awk  '/RUNNING/ {print \$(NF-1)}'`

for app in ${apps[*]} ; do
    app_name=`echo $app | awk -F. '{print \$NF}'`

    if [[ "${running_apps[@]}" =~ "$app_name" ]]; then
        echo "$app_name 已经启动,无需重复启动...."
    else
         echo "启动应用: $app_name"
        $flink run -d -c $app $jar
    fi
done




