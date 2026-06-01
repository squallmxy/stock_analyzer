# 📊 股票量化分析系统

基于 Flask 的股票智能分析平台，支持实时行情、技术指标分析、量化回测、走势预测。

## 功能模块

| 模块 | 功能 |
|------|------|
| 股票分析 | 70+自选股技术指标扫描、多因子评分、买卖信号 |
| 大盘分析 | 指数行情、板块热度、龙虎榜、资金流向、市场消息 |
| 走势预测 | 基于多因子模型的次日大盘走势预测 |
| 量化回测 | 6种策略回测、夏普/卡玛比率、权益曲线 |
| 全量分析 | 一键对所有自选股并行回测排名 |
| 自选管理 | 在线添加/删除/批量导入股票 |

## 快速部署

### 方式一：一键脚本

```bash
# 启动
bash start.sh

# 停止
bash stop.sh
```

### 方式二：Docker

```bash
# 构建镜像
docker build -t stock-analyzer .

# 运行容器
docker run -d -p 8888:8888 --name stock-analyzer stock-analyzer

# 停止
docker stop stock-analyzer
```

### 方式三：手动启动

```bash
# 安装依赖
pip install -r requirements.txt

# 启动服务
nohup python server.py > /tmp/stock_server.log 2>&1 < /dev/null &
```

## 访问

```
http://localhost:8888/stock_analysis
```

## 系统要求

- Python 3.9+
- Linux / macOS
- 网络连接（数据来自腾讯财经、东方财富公开API）

## 目录结构

```
stockanalyzer/
├── server.py           # Flask 后端
├── templates/
│   └── index.html      # 前端页面
├── 自选股票.md         # 自选股列表
├── requirements.txt    # Python依赖
├── start.sh           # 启动脚本
├── stop.sh            # 停止脚本
├── Dockerfile         # Docker构建
└── README.md          # 本文档
```
