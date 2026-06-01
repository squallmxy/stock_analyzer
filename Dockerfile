FROM python:3.11-slim

WORKDIR /app

# 安装依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY server.py .
COPY templates/ templates/
COPY 自选股票.md .

# 暴露端口
EXPOSE 8888

# 启动命令
CMD ["python", "server.py"]
