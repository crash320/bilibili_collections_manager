## B站个人收藏数据本地管理

### 运行环境
win11、python3、edge

#### Edge
准备edge浏览器文件和浏览器文件，并在`config.yaml`中配置好edge_path和driver_path
```yaml
# Edge浏览器配置
edge:
  debug_shortcut: "C:\\Users\\Public\\Desktop\\msedge_debug.lnk"
  driver_path: "D:\\04_Tools\\edgedriver_133.0.3065.82_win64\\msedgedriver.exe"
  debug_port: 9222
```

### 下载收藏视频和相关信息到本地
```shell
python bili_collect.py
```

### 本地可视化视频和评论等信息
```shell
python display_data.py
```

### 目录结构
```shell
├─cache
├─data
│  └─user_id
│      ├─bvid
├─logs
```
