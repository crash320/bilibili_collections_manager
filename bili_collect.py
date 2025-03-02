import requests
import os
import json
import re
import time
import random
import hashlib
import base64
import hmac
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from functools import partial
from utils import load_config, setup_logging, retry_on_failure, DownloadQueue
import logging

def setup_logging(config):
    """设置日志系统"""
    # 创建logs目录
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    # 生成日志文件名，格式：bili_collect_YYYY-MM-DD_HH-MM-SS.log
    timestamp = time.strftime('%Y-%m-%d_%H-%M-%S')
    log_file = os.path.join(log_dir, f'bili_collect_{timestamp}.log')
    
    # 创建日志记录器
    logger = logging.getLogger('BilibiliCollector')
    logger.setLevel(logging.INFO)
    
    # 创建文件处理器 - 记录所有日志
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    
    # 创建控制台处理器 - 使用彩色输出并简化格式
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # 定义彩色输出格式
    class ColorFormatter(logging.Formatter):
        """自定义彩色日志格式器"""
        
        grey = "\x1b[38;21m"
        blue = "\x1b[34;21m"
        yellow = "\x1b[33;21m"
        red = "\x1b[31;21m"
        bold_red = "\x1b[31;1m"
        reset = "\x1b[0m"

        def __init__(self):
            super().__init__()
            self.FORMATS = {
                logging.DEBUG: self.grey + "🔍 %(message)s" + self.reset,
                logging.INFO: self.blue + "ℹ️ %(message)s" + self.reset,
                logging.WARNING: self.yellow + "⚠️ %(message)s" + self.reset,
                logging.ERROR: self.red + "❌ %(message)s" + self.reset,
                logging.CRITICAL: self.bold_red + "🆘 %(message)s" + self.reset
            }

        def format(self, record):
            log_fmt = self.FORMATS.get(record.levelno)
            formatter = logging.Formatter(log_fmt)
            return formatter.format(record)

    console_handler.setFormatter(ColorFormatter())
    
    # 添加处理器到记录器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # 记录启动信息
    logger.info('='*50)
    logger.info('程序启动')
    logger.info(f'配置信息已保存到日志文件')
    logger.info('='*50)
    
    # 详细配置信息只写入文件
    file_handler.handle(
        logging.LogRecord(
            'BilibiliCollector', logging.INFO, '', 0,
            f'配置信息: {json.dumps(config, ensure_ascii=False, indent=2)}',
            (), None
        )
    )
    
    return logger

class BilibiliCollector:
    def __init__(self):
        self.config = load_config()
        self.logger = setup_logging(self.config)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.session = requests.Session()
        self.session.headers = self.headers
        self.download_queue = DownloadQueue(self.config['download']['max_concurrent'])

    def login(self):
        """登录B站获取cookie"""
        os.startfile(self.config['edge']['debug_shortcut'])
        options = Options()  # 得到edge的设置
        options.add_experimental_option(
            "debuggerAddress", f"127.0.0.1:{self.config['edge']['debug_port']}")  # 配置浏览器的端口地址
        # options.add_experimental_option('excudeSwitches',['enable-automation'])
        driver = webdriver.Edge(service=Service(
            self.config['edge']['driver_path']), options=options)  # 浏览器驱动的放置地址
        
        driver.get("https://passport.bilibili.com/login")
        time.sleep(3)
        # 等待登录完成
        WebDriverWait(driver, 300).until(EC.url_contains("https://www.bilibili.com/"))
        cookies = driver.get_cookies()
        for cookie in cookies:
            self.session.cookies.set(cookie['name'], cookie['value'])
        driver.quit()

    def get_collection_list(self, mid):
        """获取用户收藏夹列表"""
        url = f"https://api.bilibili.com/x/v3/fav/folder/created/list-all?up_mid={mid}"
        response = self.session.get(url)
        self.logger.info(f"get collection_list: {url}")
        return response.json()

    def get_collection_content(self, media_id, pn=1):
        """获取收藏夹内容"""
        url = f"https://api.bilibili.com/x/v3/fav/resource/list?media_id={media_id}&pn={pn}&ps=20"
        response = self.session.get(url)
        self.logger.info(f"get collection_content: {url}")
        return response.json()

    def download_cover(self, url, aid):
        """下载视频封面"""
        response = self.session.get(url)
        self.logger.info(f"get download_cover: {url}")
        if not os.path.exists('covers'):
            os.makedirs('covers')
        with open(f'covers/{aid}.jpg', 'wb') as f:
            f.write(response.content)

    def get_comments(self, aid):
        """获取视频评论"""
        try:
            url = f"https://api.bilibili.com/x/v2/reply?pn=1&type=1&oid={aid}"
            response = self.session.get(url)
            if response.status_code != 200:
                self.logger.error(f"获取评论失败，状态码: {response.status_code}")
                return {"code": -1, "message": "请求失败", "data": []}
            
            # 检查响应状态和内容
            if not response.ok:
                self.logger.warning(f"获取评论请求失败: {response.status_code}")
                return {"code": response.status_code, "message": "请求失败", "data": []}

            # 检查响应内容是否为空
            content = response.text.strip()
            if not content:
                self.logger.error("获取评论响应为空")
                return {"code": -1, "message": "响应为空", "data": []}
            
            # 尝试解析 JSON
            try:
                data = response.json()
                if data.get('code') == -404:  # 评论已关闭
                    return {"code": -404, "message": "评论功能已关闭", "data": []}
                elif data.get('code') != 0:
                    self.logger.error(f"获取评论API返回错误: {data.get('message')}")
                    return {"code": data.get('code'), "message": data.get('message'), "data": []}
                return data
            except json.JSONDecodeError as e:
                self.logger.warning(f"评论JSON解析失败: {str(e)}, 内容: {content[:100]}")
                return {"code": -1, "message": "JSON解析失败", "data": []}
        except Exception as e:
            self.logger.warning(f"获取评论异常: {str(e)}")
            return {"code": -1, "message": str(e), "data": []}

    def get_danmaku(self, cid):
        """获取视频弹幕"""
        try:
            url = f"https://api.bilibili.com/x/v1/dm/list.so?oid={cid}"
            response = self.session.get(url)

            # 处理特殊状态码
            if response.status_code == 412:
                return "<i>弹幕获取被拦截，需要验证</i>"
            elif response.status_code != 200:
                return f"<i>获取弹幕失败: {response.status_code}</i>"
            elif response.status_code != 200:
                self.logger.error(f"获取弹幕失败，状态码: {response.status_code}")
                return "<i>弹幕获取失败</i>"
            
            # 检查响应内容是否为空
            if not response.content:
                self.logger.error("获取弹幕响应为空")
                return "<i>弹幕内容为空</i>"
            
            return response.content.decode('utf-8')
        except Exception as e:
            return f"<i>获取弹幕出错: {str(e)}</i>"

    @retry_on_failure(max_retries=3, delay=2)
    def download_video(self, aid, bvid, video_path):
        """下载视频到指定路径"""
        try:
            # 检查视频文件是否有效
            if os.path.exists(video_path):
                if os.path.getsize(video_path) > 1024 * 1024:
                    self.logger.info(f"视频已存在且有效，跳过下载: {video_path}")
                    return
                else:
                    #打开视频查看是否可以解码
                    try:
                        import ffmpeg
                        ffmpeg.probe(video_path)
                        self.logger.info(f"视频已存在且有效，跳过下载: {video_path}")
                        return
                    except Exception as e:
                        self.logger.warning(f"视频已存在但文件大小异常，删除并重新下载: {video_path}")
                        os.remove(video_path)

            # 获取cid
            cid_url = f"https://api.bilibili.com/x/web-interface/view?bvid={bvid}"  # 使用bvid替代aid
            cid_response = self.session.get(cid_url, timeout=self.config['request']['timeout'])
            cid_data = cid_response.json()
            if cid_data['code'] != 0:
                raise Exception(f"获取cid失败: {cid_data['message']}")
            
            cid = cid_data['data']['cid']
            
            # 获取视频下载地址
            quality = self.config['download']['video_quality']
            # 更新请求URL和参数
            url = f"https://api.bilibili.com/x/player/playurl"
            params = {
                'avid': aid,
                'bvid': bvid,
                'cid': cid,
                'qn': quality,
                'fnval': 16,  # 支持dash格式
                'fnver': 0,
                'fourk': 1
            }
            
            # 更新请求头
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Referer': f'https://www.bilibili.com/video/{bvid}',
                'Origin': 'https://www.bilibili.com',
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'zh-CN,zh;q=0.9',
                'Cookie': '; '.join([f'{k}={v}' for k, v in self.session.cookies.items()])
            }
            # 禁用代理
            original_proxies = self.session.proxies
            self.session.proxies = {}

            try:    
                response = self.session.get(url, params=params, headers=headers, timeout=30)
                
                self.logger.info(f"get download_video: {url}")

                if response.status_code != 200:
                    raise Exception(f"获取视频地址失败: {response.status_code}")
                
                video_data = response.json()
                
                self.logger.info(f"get download_video: {video_data}")
                
                # 获取最高质量的视频地址
                if 'data' in video_data and 'dash' in video_data['data']:
                    # DASH格式
                    video_url = video_data['data']['dash']['video'][0]['baseUrl']
                else:
                    # 传统格式
                    video_url = video_data['data']['durl'][0]['url']
                
                # 支持断点续传
                headers['Range'] = 'bytes=0-'  # 添加Range头
                temp_path = f"{video_path}.tmp"
                downloaded_size = 0
            
                if os.path.exists(temp_path):
                    downloaded_size = os.path.getsize(temp_path)
                    headers['Range'] = f'bytes={downloaded_size}-'
                
                # 使用较小的chunk_size以提高稳定性
                chunk_size = 512 * 1024  # 512KB
                response = self.session.get(video_url, stream=True, headers=headers, timeout=60)
            
                # 检查响应状态
                if response.status_code not in [200, 206]:
                    raise Exception(f"下载视频失败，状态码: {response.status_code}")
                
                # 获取总大小
                total_size = int(response.headers.get('content-length', 0)) + downloaded_size
                
                if total_size < 1024 * 1024:  # 小于1MB可能是错误响应
                    raise Exception("视频大小异常，可能是无效响应")
                
                mode = 'ab' if downloaded_size > 0 else 'wb'
                with open(temp_path, mode) as f:
                    for chunk in response.iter_content(chunk_size=self.config['download']['chunk_size']):
                        if chunk:
                            f.write(chunk)
                            downloaded_size += len(chunk)
                            self.logger.info(f"下载进度: {downloaded_size}/{total_size} ({(downloaded_size/total_size)*100:.2f}%)")
            
                # 下载完成后检查文件大小
                if abs(os.path.getsize(temp_path) - total_size) > total_size * 0.01:
                    raise Exception("文件大小不匹配，下载可能不完整")
                
                # 修改文件重命名逻辑
                if os.path.exists(video_path):
                    os.remove(video_path)  # 如果目标文件已存在，先删除
                os.rename(temp_path, video_path)
                self.logger.info(f"视频下载完成: {video_path}")
            finally:
                # 恢复代理
                self.session.proxies = original_proxies
            
        except Exception as e:
            self.logger.error(f"下载视频失败 {bvid}: {str(e)}")
            # 清理可能的临时文件
            if os.path.exists(f"{video_path}.tmp"):
                os.remove(f"{video_path}.tmp")
            raise

    def save_info(self, data):
        """保存视频信息"""
        if not os.path.exists('info'):
            os.makedirs('info')
        with open(f'info/{data["aid"]}.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    def process_video(self, cache_dir, item):
        """处理单个视频的所有信息"""
        try:
            video_id = item['id']
            bvid = item['bvid']
            title = item['title']
        
            # 检查视频是否可访问
            check_url = f"https://api.bilibili.com/x/web-interface/view?bvid={bvid}"
            check_response = self.session.get(check_url)
            self.logger.info(f"get check_url: {check_url}")
            # 检查响应状态
            if check_response.status_code != 200:
                if isinstance(check_response.json(), dict) and "message" in check_response.json():
                    self.logger.warning(f"视频不可访问，跳过: 错误信息 {check_response.json()['message']} 标题 {title}")
                else:
                    self.logger.warning(f"视频不可访问，跳过: 错误信息 {check_response} 标题 {title}")
                return
            
            video_cache_dir = os.path.join(cache_dir, str(video_id))
            os.makedirs(video_cache_dir, exist_ok=True)

            # 简化的终端输出
            self.logger.info(f"处理视频: {title[:20]}{'...' if len(title) > 20 else ''}")
            
            video_data = {
                'title': title,
                'id': video_id,
                'bvid': bvid,
                'desc': item.get('intro', ''),
                'cover': item['cover']
            }
            
            try:
                # 下载封面
                cover_path = os.path.join(video_cache_dir, f'cover.jpg')
                if not os.path.exists(cover_path):
                    self.logger.debug(f"下载封面: {video_id}")  # 降低日志级别
                    self.logger.info(f"下载视频封面: {video_id}")
                    self.logger.info(f"get item['cover']: {item['cover']}")
                    response = self.session.get(item['cover'])
                    if response.status_code == 200:
                        with open(cover_path, 'wb') as f:
                            f.write(response.content)
                    else:
                        self.logger.error(f"下载封面失败: {response.status_code}")
            except Exception as e:
                self.logger.error(f"下载封面失败: {str(e)}")
            
            # 获取评论（忽略错误）
            try:
                comments_path = os.path.join(video_cache_dir, 'comments.json')
                if not os.path.exists(comments_path):
                    self.logger.debug(f"获取评论: {video_id}")  # 降低日志级别
                    comments = self.get_comments(video_id)
                    # 只有在成功获取评论时才写入文件
                    if comments and comments.get('code') == 0 and isinstance(comments, dict):
                        with open(comments_path, 'w', encoding='utf-8') as f:
                            json.dump(comments, f, ensure_ascii=False, indent=4)
                else:
                    # 读取现有评论文件
                    try:
                        with open(comments_path, 'r', encoding='utf-8') as f:
                            content = f.read().strip()
                            if content:  # 确保文件不为空
                                comments = json.loads(content)
                            else:
                                comments = {"code": -1, "message": "缓存文件为空", "data": []}
                    except (json.JSONDecodeError, Exception) as e:
                        self.logger.warning(f"读取评论缓存失败: {str(e)}")
                        comments = {"code": -1, "message": f"读取缓存失败: {str(e)}", "data": []}
            except Exception as e:
                self.logger.warning(f"获取评论失败（非致命错误）: {str(e)}")
                comments = {"code": -1, "message": str(e), "data": []}
            video_data['comments'] = comments

            # 获取弹幕（忽略错误）
            try:
                cid = item['ugc']['first_cid']
                danmaku_path = os.path.join(video_cache_dir, 'danmaku.xml')
                if not os.path.exists(danmaku_path):
                    self.logger.debug(f"获取弹幕: {video_id}")  # 降低日志级别
                    danmaku = self.get_danmaku(cid)
                    if danmaku and not danmaku.startswith('<html>'): # 只在获取成功时保存
                        with open(danmaku_path, 'w', encoding='utf-8') as f:
                            f.write(danmaku)
                else:
                    try:
                        with open(danmaku_path, 'r', encoding='utf-8') as f:
                            danmaku = f.read()
                    except Exception as e:
                        danmaku = f"<i>读取缓存弹幕失败: {str(e)}</i>"
            except Exception as e:
                self.logger.warning(f"获取弹幕失败（非致命错误）: {str(e)}")
                danmaku = f"<i>获取弹幕失败: {str(e)}</i>"

            video_data['danmaku'] = danmaku
            
            try:
                # 将视频下载任务添加到队列
                video_path = os.path.join(video_cache_dir, 'video.mp4')
                if not os.path.exists(video_path):
                    self.logger.info(f"添加下载任务: {title[:20]}{'...' if len(title) > 20 else ''}")
                    self.download_queue.add_task(
                        lambda: self.download_video(video_id, bvid, video_path)
                        )
            except Exception as e:
                self.logger.error(f"添加下载任务失败: {str(e)}")
                
            try:
                # 保存信息
                info_path = os.path.join(video_cache_dir, 'info.json')
                with open(info_path, 'w', encoding='utf-8') as f:
                    json.dump(video_data, f, ensure_ascii=False, indent=4)
                
                self.logger.debug(f"视频信息处理完成: {video_id}")  # 降低日志级别
                
            except Exception as e:
                self.logger.error(f"保存视频信息失败 [{title[:20]}...]: {str(e)}")
                raise
        except Exception as e:
            self.logger.error(f"处理视频失败 [{title}]: {str(e)}")
            return False # 继续处理下一个视频而不是抛出异常

    def save_login_info(self):
        """保存登录信息到缓存文件"""
        cache_dir = 'cache'
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir, exist_ok=True)
        with open(os.path.join(cache_dir, 'login_info.json'), 'w', encoding='utf-8') as f:
            json.dump(self.session.cookies.get_dict(), f, ensure_ascii=False, indent=4)

    def load_login_info(self):
        """从缓存文件加载登录信息，返回是否加载成功"""
        cache_dir = 'cache'
        cache_file = os.path.join(cache_dir, 'login_info.json')
        if not os.path.exists(cache_file):
            return False
            
        with open(cache_file, 'r', encoding='utf-8') as f:
            login_info = json.load(f)
            self.session.cookies.update(login_info)
            
        # 验证登录状态
        response = self.session.get("https://www.bilibili.com/")
        return response.status_code == 200

    def get_user_mid(self):
        """从缓存获取用户mid，如果不存在则请求输入并保存"""
        cache_dir = 'cache'
        cache_file = os.path.join(cache_dir, 'mid.txt')
        
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir, exist_ok=True)
            
        if not os.path.exists(cache_file):
            mid = input("请输入用户mid：")
            with open(cache_file, 'w', encoding='utf-8') as f:
                f.write(mid)
        else:
            with open(cache_file, 'r', encoding='utf-8') as f:
                mid = f.read().strip()
                
        return mid

    def get_cached_data(self, force_refresh, cache_dir, cache_filename, fetch_func, *args):
        """
        通用缓存数据获取函数
        
        参数:
            force_refresh: 是否强制刷新缓存
            cache_dir: 缓存目录
            cache_filename: 缓存文件名
            fetch_func: 获取数据的函数
            args: fetch_func的参数
            
        返回:
            缓存的数据或新获取的数据
        """
        try:
            cache_file = os.path.join(cache_dir, cache_filename)
            
            if os.path.exists(cache_file) and not force_refresh:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                data = fetch_func(*args)
                # 检查API返回是否成功
                if isinstance(data, dict) and data.get('code') != 0:
                    raise Exception(f"API请求失败: {data.get('message')}")
            
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
                return data
        except Exception as e:
            self.logger.error(f"获取数据失败: {str(e)}")
            return None

    def main(self):
        try:
            self.logger.info('开始收藏夹下载任务')
            
            # 尝试加载已保存的登录信息，如果失败则重新登录
            if not self.load_login_info():
                self.login()
                self.save_login_info()

            self.logger.info('登录成功!')

            # 获取用户mid
            mid = self.get_user_mid()

            # 针对mid创建缓存目录
            cache_dir = os.path.join(os.path.dirname(__file__), 'data', mid)
            os.makedirs(cache_dir, exist_ok=True)

            # 获取收藏夹列表，添加force_refresh参数
            force_refresh = False
            collections = self.get_cached_data(force_refresh, cache_dir, 'collections.json', 
                                             self.get_collection_list, mid)
            if not collections:
                self.logger.warning("首次获取收藏夹列表失败，尝试强制刷新...")
                force_refresh = True
                collections = self.get_cached_data(force_refresh, cache_dir, 'collections.json', 
                                                 self.get_collection_list, mid)
                if not collections:
                    raise Exception("获取收藏夹列表失败")
            
            # 遍历收藏夹
            for folder in collections['data']['list']:
                self.logger.info(f"收藏夹: {folder['title']}")
                media_id = folder['id']
                pn = 1
                
                while True: 
                    content = self.get_cached_data(force_refresh, cache_dir, 
                                                 f'{media_id}_{pn}.json',
                                                 self.get_collection_content, 
                                                 media_id, pn)
                    
                    if not content or not content['data']['medias']:
                        break
                        
                    self.logger.info(f"处理第 {pn} 页视频")
                    
                    with ThreadPoolExecutor(max_workers=4) as executor:
                        futures = []
                        for media in content['data']['medias']:
                            # 检查必要的字段
                            required_fields = ['id', 'bvid', 'title', 'cover', 'ugc']
                            if not all(field in media for field in required_fields):
                                self.logger.error(f"媒体数据缺少必要字段: {media}")
                                continue
                            
                            self.logger.info(f"准备处理视频: {media['title']} (id: {media['id']})")
                            future = executor.submit(self.process_video, cache_dir, media)
                            futures.append(future)
                        
                        # 等待所有任务完成并检查是否有异常
                        for future in futures:
                            try:
                                future.result()
                            except Exception as e:
                                self.logger.error(f"处理视频时发生错误: {str(e)}")
                    
                    self.logger.info(f"第{pn}页视频处理完成")
                    pn += 1
                    time.sleep(1)  # 避免请求过快

            # 在所有收藏夹处理完成后，等待下载队列完成
            self.logger.info("等待所有下载任务完成...")
            self.download_queue.queue.join()
            self.download_queue.stop()
            self.logger.info("所有任务已完成!")
                
            self.logger.info('='*50)
            self.logger.info('程序执行完成')
            self.logger.info('='*50)
                
        except Exception as e:
            self.logger.error('程序执行出错')
            self.logger.error(str(e))
            self.logger.info('='*50)
            raise

if __name__ == "__main__":
    collector = BilibiliCollector()
    collector.main()
