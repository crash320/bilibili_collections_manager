import pandas as pd
import os
import json
from pathlib import Path
import streamlit as st

import plotly.express as px
import plotly.graph_objects as go

class VideoDataManager:
    def __init__(self, data_path):
        self.data_path = Path(data_path)
        self.videos_df = None
        self.comments_df = None
        self.load_data()
    
    def load_data(self):
        # 遍历数据目录下的所有视频文件夹
        videos_data = []
        comments_data = []
        
        # 获取用户ID文件夹
        user_folders = [f for f in self.data_path.iterdir() if f.is_dir()]
        
        for user_folder in user_folders:
            # 遍历每个视频文件夹
            video_folders = [f for f in user_folder.iterdir() if f.is_dir()]
            
            for video_folder in video_folders:
                # 读取info.json
                info_path = video_folder / 'info.json'
                if info_path.exists():
                    with open(info_path, 'r', encoding='utf-8') as f:
                        info = json.load(f)
                        videos_data.append({
                            'video_id': info['id'],
                            'bvid': info['bvid'],
                            'title': info['title'],
                            'description': info['desc'],
                            'cover_path': str(video_folder / 'cover.jpg'),
                            'partition': '未分类'  # 可以根据需要添加分区信息
                        })
                    
                    # 读取comments.json
                    comments_path = video_folder / 'comments.json'
                    if comments_path.exists():
                        with open(comments_path, 'r', encoding='utf-8') as f:
                            comment_data = json.load(f)
                            
                            # 处理评论数据
                            if comment_data.get('code') == 0 and 'data' in comment_data:
                                replies = comment_data['data'].get('replies', [])
                                
                                # 处理置顶评论
                                top_comment = comment_data['data'].get('upper', {}).get('top')
                                if top_comment:
                                    replies = [top_comment] + replies
                                
                                # 处理所有评论
                                for reply in replies:
                                    if isinstance(reply, dict):
                                        # 提取评论内容
                                        comment_content = reply.get('content', {}).get('message', '')
                                        
                                        # 提取用户信息
                                        member = reply.get('member', {})
                                        username = member.get('uname', '未知用户')
                                        
                                        # 提取时间信息
                                        time_desc = reply.get('reply_control', {}).get('time_desc', '')
                                        
                                        # 提取位置信息
                                        location = reply.get('reply_control', {}).get('location', '')
                                        
                                        comments_data.append({
                                            'video_id': info['id'],
                                            'comment': comment_content,
                                            'user': username,
                                            'time': time_desc,
                                            'location': location,
                                            'is_top': bool(reply.get('reply_control', {}).get('is_up_top'))
                                        })
                                        
                                        # 处理回复的评论
                                        sub_replies = reply.get('replies', [])
                                        for sub_reply in sub_replies:
                                            if isinstance(sub_reply, dict):
                                                sub_content = sub_reply.get('content', {}).get('message', '')
                                                sub_member = sub_reply.get('member', {})
                                                sub_username = sub_member.get('uname', '未知用户')
                                                sub_time = sub_reply.get('reply_control', {}).get('time_desc', '')
                                                sub_location = sub_reply.get('reply_control', {}).get('location', '')
                                                
                                                comments_data.append({
                                                    'video_id': info['id'],
                                                    'comment': sub_content,
                                                    'user': sub_username,
                                                    'time': sub_time,
                                                    'location': sub_location,
                                                    'is_top': False
                                                })
                            else:
                                # 评论获取失败的情况
                                comments_data.append({
                                    'video_id': info['id'],
                                    'comment': f"评论获取失败: {comment_data.get('message', '未知错误')}",
                                    'user': '系统',
                                    'time': '',
                                    'location': '',
                                    'is_top': False
                                })
        
        self.videos_df = pd.DataFrame(videos_data)
        self.comments_df = pd.DataFrame(comments_data)

class VideoClassifier:
    def __init__(self):
        self.tags = {}
        self.load_tags()
    
    def load_tags(self):
        # 从本地文件加载已有标签
        if os.path.exists('tags.json'):
            with open('tags.json', 'r', encoding='utf-8') as f:
                self.tags = json.load(f)
    
    def add_tag(self, video_id, tag):
        if video_id not in self.tags:
            self.tags[video_id] = []
        if tag not in self.tags[video_id]:
            self.tags[video_id].append(tag)
        self.save_tags()
    
    def save_tags(self):
        with open('tags.json', 'w', encoding='utf-8') as f:
            json.dump(self.tags, f, ensure_ascii=False, indent=2)


def create_dashboard():
    st.title("B站视频数据管理系统")
    
    # 侧边栏：筛选条件
    st.sidebar.header("筛选条件")
    
    # 按标题搜索
    search_title = st.sidebar.text_input("搜索视频标题")
    
    # 按播放量范围筛选（如果有播放量数据）
    if 'view_count' in data_manager.videos_df.columns:
        view_range = st.sidebar.slider(
            "播放量范围",
            min_value=int(data_manager.videos_df['view_count'].min()),
            max_value=int(data_manager.videos_df['view_count'].max()),
            value=(0, int(data_manager.videos_df['view_count'].max()))
        )
    
    # 主界面：数据展示
    st.header("视频列表")
    
    # 筛选数据
    filtered_df = data_manager.videos_df.copy()
    if search_title:
        filtered_df = filtered_df[filtered_df['title'].str.contains(search_title, case=False, na=False)]
    
    # 显示视频列表
    for _, video in filtered_df.iterrows():
        with st.expander(f"{video['title']}"):
            col1, col2 = st.columns([1, 2])
            
            # 显示封面
            if os.path.exists(video['cover_path']):
                col1.image(video['cover_path'])
            
            # 显示视频信息
            col2.write(f"BV号：{video['bvid']}")
            col2.write(f"描述：{video['description']}")
            
            # 显示评论
            if not data_manager.comments_df.empty:
                video_comments = data_manager.comments_df[
                    data_manager.comments_df['video_id'] == video['video_id']
                ]
                if not video_comments.empty:
                    st.write("评论：")
                    for _, comment in video_comments.iterrows():
                        st.text(f"{comment['user']}: {comment['comment']}")

def plot_partition_distribution():
    partition_counts = data_manager.videos_df['partition'].value_counts()
    fig = px.pie(
        values=partition_counts.values,
        names=partition_counts.index,
        title="视频分区分布"
    )
    st.plotly_chart(fig)

def plot_view_distribution():
    fig = px.histogram(
        data_manager.videos_df,
        x='view_count',
        title="播放量分布"
    )
    st.plotly_chart(fig)

def analyze_video_trends():
    # 按时间统计视频发布趋势
    videos_by_date = data_manager.videos_df.groupby('publish_date').size()
    fig = px.line(
        x=videos_by_date.index,
        y=videos_by_date.values,
        title="视频发布趋势"
    )
    st.plotly_chart(fig)

def filter_videos(partitions, view_range):
    filtered_df = data_manager.videos_df.copy()
    
    # 按分区筛选
    if partitions:
        filtered_df = filtered_df[filtered_df['partition'].isin(partitions)]
    
    # 按播放量筛选
    filtered_df = filtered_df[
        (filtered_df['view_count'] >= view_range[0]) & 
        (filtered_df['view_count'] <= view_range[1])
    ]
    
    return filtered_df

if __name__ == "__main__":
    data_manager = VideoDataManager("./data")
    classifier = VideoClassifier()
    
    st.set_page_config(page_title="B站视频数据管理系统", layout="wide")
    create_dashboard()