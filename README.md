# my-lab6824

#### 介绍
mit6.824 分布式系统课程 实验作业
[课程主页](https://pdos.csail.mit.edu/6.824/schedule.html)
课程要求尽量自己完成。欢迎在完成后或遇到困难时查看代码，探讨交流~

#### 已完成
1. Lab1: MapReduce, 已通过 `test-mr.sh` 测试。Lab1介绍: [6.824 Lab 1: MapReduce](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)
    - note: 实现代码在src/mr下。
    - 代码要求实验系统为linux、FreeBSD、mac。否则无法使用golang的plugin库。 
    > Currently plugins are only supported on Linux, FreeBSD, and macOS. Please report any issues

    - 在 `test-mr.sh` 的 early exit test 第246行, 原来为`wait -n`。 由于`-n`参数只在bash版本4.3+才有, 本人服务器bash版本不足, 因此去掉了`-n`，否则该测试在代码正确情况下无法通过。可通过 `/bin/bash -version`查看bash版本。
