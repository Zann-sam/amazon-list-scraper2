# Amazon UK Trend Scout 部署说明

## 方式一：Streamlit Community Cloud（推荐）

1. 将本项目推到 GitHub（确保包含 `app.py` 与 `requirements.txt`）。
2. 打开 https://share.streamlit.io/ 并登录。
3. 点击 **Create app**，选择仓库与分支，入口文件填写 `app.py`。
4. 在 **Advanced settings -> Secrets** 添加：

```toml
FLYBY_API_KEY = "你的key"
FLYBY_API_HOST = "real-time-amazon-data-the-most-complete.p.rapidapi.com"
FLYBY_API_BASE_URL = "https://real-time-amazon-data-the-most-complete.p.rapidapi.com"
```

5. 点击 Deploy。

## 注意事项

- `favorites.json`、`new_releases_history.json` 属于本地文件存储，云端重启后可能重置。
- 如需长期保存收藏/历史快照，建议后续改为数据库（如 SQLite/Postgres/Supabase）。
