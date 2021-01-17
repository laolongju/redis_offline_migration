# redis_offline_migration
从阿里云一键迁移Redis到AWS, 提供了 Jupyter Notebook 供参考

## 功能
1. 支持跨版本迁移，已验证从阿里☁️ 2.8/4.0 版本可以成功迁移到AWS 4.0.10 版本；
2. 阿里☁️ AccessKey 安全保存在 AWS Systems Manager Parameter Store；

## 性能
迁移 3.5GB 内存数据需要 10 分 20 秒。

## 注意
1. AWS 2.8 版本不能开启 TLS/AES, 而 AUTHTOKEN 依赖于 TLS；
2. 建议运行在 EC2 上，给予绑定的 Role创建 Redis 实例和上传 S3 文件以及读 SSM Parameter Store 的权限
