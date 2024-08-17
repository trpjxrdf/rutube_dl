import requests
import os

class RutubeDl:

    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/98.0.4758.132 YaBrowser/22.3.1.892 Yowser/2.5 Safari/537.36',
        'accept': '*/*'
    }

    def __init__(self, video_id:str, max_retries:int=10):
        self.video_id = video_id
        self.max_retries = max_retries
        self.load_m3u8_list(video_id)

    def _get_with_retries(self, url):
        for i in range(0, self.max_retries-1):
            try:
                return requests.get(url=url, headers=RutubeDl.headers)
            except Exception as e:
                print(str(e))
        return requests.get(url=url, headers=RutubeDl.headers)

    def load_m3u8_list(self, video_id):
        req = self._get_with_retries(f'https://rutube.ru/api/play/options/{video_id}/?no_404=true&referer=https%3A%2F%2Frutube.ru')
        req.raise_for_status()

        self.info = req.json()
        video_author = self.info['author']['name']
        video_title = self.info['title']
        dict_repl = ["/", "\\", "[", "]", "?", "'", '"', ":", "."]
        for repl in dict_repl:
            if repl in video_title:
                video_title = video_title.replace(repl, "")
            if repl in video_author:
                video_author = video_author.replace(repl, "")
        self.video_title = video_title.replace(" ", "_")
        self.video_author = video_author.replace(" ", "_")
        self.video_url = self.info['video_balancer']['m3u8']

    def parse_codec_info(self, info):
        res = {}
        i = 0
        while i < len(info):
            while i < len(info) and info[i] == ' ':
                i += 1
            if i >= len(info):
                break
            i1 = i
            value = None
            name = None
            while i < len(info) and info[i] != '=':
                i += 1
            if i >= len(info):
                raise Exception(f'"=" is expected: ' + info[i1:])
            name = info[i1:i]
            i += 1
            if i >= len(info):
                break
            if info[i] == '"':
                i += 1
                i1 = i
                while i < len(info) and info[i] != '"':
                    i += 1
                if i >= len(info):
                    raise Exception(f'Unclosed double quote: ' + info[i1-1:])
                value = info[i1:i]
                while i < len(info) and info[i] != ',':
                    i += 1
                i += 1
            else:
                i1 = i
                while i < len(info) and info[i] != ',':
                    i += 1
                value = info[i1:i]
                i += 1
            res[name] = value
        return res

    def list_formats(self):
        req = self._get_with_retries(self.video_url)
        req.raise_for_status()

        res = []
        lines = req.text.split('\n')
        token = '#EXTM3U'
        if lines[0] != token:
            raise Exception(f'"{token}" is expected: ' + lines[0])
        i = 1
        while i < len(lines):
            if lines[i] == '':
                break
            token = '#EXT-X-STREAM-INF:'
            while i < len(lines) and not lines[i].startswith(token):
                i += 1
            if i >= len(lines):
                raise Exception(f'"{token}" is not found')
            codec_info = lines[i]
            i += 1
            token = 'https://'
            while i < len(lines) and not lines[i].startswith(token):
                i += 1
            if i >= len(lines):
                raise Exception(f'"{token}" is not found')
            link_url = lines[i]
            i += 1
            d = self.parse_codec_info(codec_info[18:])
            d['url'] = link_url
            res.append(d)

        return res

    def get_download_url(self, fmt):
        url = fmt['url']
        i = len(url) - 1
        while i >= 0 and url[i] != '?':
            i -= 1
        if i >= 0:
            url = url[:i]
        if not url.endswith('.m3u8'):
            raise Exception(f'url must ends with .m3u8: {url}')
        return url[0:-5]

    def _load_segments(self, folder, fmt):
        link = self.get_download_url(fmt)
        #count = self.get_segment_count(fmt)
        i = 1
        with open(os.path.join(folder, 'merged.ts'), 'wb') as merged:
            while True:
                print(f'[+] - Загружаю сегмент {i}')
                segment_name = f'segment-{i}-v1-a1.ts'
                req = self._get_with_retries(f'{link}/{segment_name}')
                if req.status_code == 404:
                    break
                req.raise_for_status()
                with open(os.path.join(folder, segment_name), 'wb') as file:
                    file.write(req.content)
                merged.write(req.content)
                i += 1
                yield segment_name
        print('[INFO] - Все сегменты загружены')

    def download_to_stream(self, fmt, writer):
        link = self.get_download_url(fmt)
        #count = self.get_segment_count(fmt)
        i = 1
        while True:
#           print(f'[+] - Загружаю сегмент {i}')
            segment_name = f'segment-{i}-v1-a1.ts'
            req = self._get_with_retries(f'{link}/{segment_name}')
            if req.status_code == 404:
                break
            req.raise_for_status()
            writer(req.content)
            i += 1
            yield segment_name, len(req.content)
#        print('[INFO] - Все сегменты загружены')

    def download_to_file(self, fmt, file_name:str):
        with open(file_name, 'wb') as f:
            for s in self.download_to_stream(fmt, f.write):
                yield s

    def get_segment_count(self, fmt):
        req = self._get_with_retries(fmt['url'])
        req.raise_for_status()
        data_seg_dict = []
        for seg in req:
            data_seg_dict.append(seg)
        seg_count = str(data_seg_dict[-2]).split("/")[-1].split("-")[1]
        return seg_count

if __name__ == '__main__':

    video_id = '5ff620ac5627a5800ef32f523c0f1da1'

    dl = RutubeDl(video_id)

    fmts = dl.list_formats()
    fmt = fmts[0] # worst quality

    folder = 'D:\\5'

    for x in dl.download_to_file(fmt, os.path.join(folder, f'{dl.video_title}.ts')):
        print(x)

