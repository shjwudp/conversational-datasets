一个新的晋江文学城爬虫，用于对话数据收集，感谢晋江的开放包容！


## 快速启动

```bash
# 爬评论区
python crawl_board.py \
    --start_urls "https://bbs.jjwxc.net/board.php?board=2&type=&page=1" \
    --output "jjwxc_board.jsonl"

# 爬留言
python crawl_showmsg.py \
    --input "jjwxc_board.jsonl" \
    --output "jjwxc_message.jsonl"
```
