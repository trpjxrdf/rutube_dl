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
        self.video_url = self.info['video_balancer']['m3u8']

        self.info.pop('advert')
        self.info.pop('stat')
        self.info.pop('appearance')

        self.video_author = self.info['author']
        self.video_title = self.info['title']
        self.video_description = self.info['description']
        self.video_thumbnail_url = self.info['thumbnail_url']

    def _parse_codec_info(self, info):
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
            d = self._parse_codec_info(codec_info[18:])
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

    def get_segment_list(self, fmt):
        req = self._get_with_retries(fmt['url'])
        req.raise_for_status()
        lines = req.content.decode().split('\n')
        i = 0
        while i < len(lines):
            while i < len(lines) and not lines[i].startswith('#EXTINF:'):
                i += 1
            if i >= len(lines) - 1:
                break
            info = lines[i+1].strip()
            i += 2
            yield tuple(info.split("/"))

    def _download_to_stream_2(self, fmt, writer):
        link = self.get_download_url(fmt)
        #count = self.get_segment_count(fmt)
        i = 1
        while True:
            segment_name = f'segment-{i}-v1-a1.ts'
            req = self._get_with_retries(f'{link}/{segment_name}')
            if req.status_code == 404:
                break
            req.raise_for_status()
            writer(req.content)
            i += 1
            yield segment_name, len(req.content)

    def download_to_stream(self, fmt, writer):
        link = self.get_download_url(fmt)
        sl = [ts for mp4, ts in self.get_segment_list(fmt)]
        for i in range(0, len(sl)):
            req = self._get_with_retries(f'{link}/{sl[i]}')
            req.raise_for_status()
            writer(req.content)
            yield i+1, len(sl), sl[i], len(req.content)

    def download_to_file(self, fmt, file_name:str):
        with open(file_name, 'wb') as f:
            for s in self.download_to_stream(fmt, f.write):
                yield s

if __name__ == '__main__':

    video_id = '5ff620ac5627a5800ef32f523c0f1da1'

    dl = RutubeDl(video_id)

    fmts = dl.list_formats()

    fmt = fmts[0] # worst quality

    folder = 'D:\\5'

    for x in dl.download_to_file(fmt, os.path.join(folder, f'{dl.video_title}.ts')):
        print(x)

