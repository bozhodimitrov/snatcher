import asyncio
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stdout
from errno import ECONNRESET
from functools import partial
from types import SimpleNamespace as Show

import toml
import youtube_dl
from aioconsole.stream import aprint
from aiohttp import ClientOSError
from aiohttp import ClientPayloadError
from aiohttp import ClientSession
from aiohttp import ClientTimeout
from aiohttp import InvalidURL
from aiohttp import TCPConnector
from cachetools import LRUCache
from editdistance_s import distance
from lxml.etree import ParserError
from lxml.etree import strip_elements
from lxml.etree import XMLSyntaxError
from lxml.html import fromstring
from lxml.html import HTMLParser


HTTP_EXCEPTIONS = (
    ClientOSError,
    ClientPayloadError,
    InvalidURL,
    OSError,
    asyncio.TimeoutError,
)


class Cache(LRUCache):
    def __contains__(self, value):
        try:
            key, data = value
        except ValueError:
            key, data = value, ''

        found = super().__contains__(key)
        self[key] = data
        return found


class Wanted(set):
    def __init__(self, items):
        self.items = {item.lower(): item for item in items}

    def __contains__(self, value):
        lowercase_value = value.name.lower()
        for item in self.items:
            if item in lowercase_value or self.similar(item, lowercase_value):
                value.title = self.items[item]
                return True
        else:
            return False

    @staticmethod
    def similar(item, value):
        half_length = len(value) // 2
        quarter_length = len(value) // 4
        return any(
            distance(item[:i], value[:i]) < 5
            for i in (half_length, quarter_length)
        )


class Downloader:
    def __init__(self, config='config.toml'):
        self.config_file = config
        self.config = self.load_config()
        self.settings = self.config['settings']

        self.wanted_shows = Wanted(self.settings['shows'])
        self.workers = ThreadPoolExecutor(max_workers=self.settings['workers'])
        self.parser = HTMLParser(collect_ids=False)
        self.seen_posts = Cache(self.settings['cache'])
        self.seen_posts.update(self.config['last'])

        self.default_ytdl_opts = {
            'quiet': True,
            'continuedl': True,
            'fragment_retries': self.settings['retries'],
            'retries': self.settings['retries'],
            'format': self.settings['ytdl_format'],
            'skip_download': self.settings.get('skip_download', False),
        }

    def load_config(self):
        try:
            return toml.load(self.config_file)
        except (OSError, toml.TomlDecodeError):
            raise SystemExit('Invalid config')

    def save_seen_posts(self):
        self.config['last'].update(self.seen_posts)
        with open(self.config_file, 'w') as f:
            toml.dump(self.config, f)

    def dl_links(self, links):
        if isinstance(links, str):
            links = (links,)

        for src in self.settings['source_priority']:
            for link in links:
                if src in link:
                    yield str(link)

    def download(self, show):
        dl_path = f'{self.settings["download_dir"]}/{show.title}'
        opts = {
            **self.default_ytdl_opts,
            'outtmpl': f'{dl_path}/{youtube_dl.utils.DEFAULT_OUTTMPL}',
        }

        with redirect_stdout(sys.stderr):
            with youtube_dl.YoutubeDL(opts) as yt:
                print(f'Downloading: {show.name}', flush=True)
                for _ in range(self.settings['retries']):
                    try:
                        result = yt.download([show.url])
                    except youtube_dl.utils.DownloadError:
                        result = 1

                    if result == 0:
                        break

            if result == 0:
                print(f'Download finish: {show.name}', flush=True)
            else:
                print(f'Error while downloading: {show.url}', flush=True)

    async def extractor(self, html):
        html = html.replace('<!--', '').replace('-->', '')
        loop = asyncio.get_event_loop()

        try:
            root = await loop.run_in_executor(
                None, partial(fromstring, html, parser=self.parser),
            )
        except (ParserError, XMLSyntaxError):
            return

        content = root.xpath(self.settings['path']['content'])
        if not content:
            await asyncio.sleep(self.settings['cooldown'])
            return

        for tag in reversed(content):
            post_id = tag.attrib.get('id')
            if not post_id:
                raise SystemExit('Post ID not found')

            links = tag.xpath(self.settings['path']['links'])
            raw_links = self.dl_links(links)
            strip_elements(tag, 'a')
            names = (
                el.strip() for el in tag.xpath(self.settings['path']['name'])
            )
            name = ' '.join(el for el in names if el)

            yield post_id, raw_links, name

    async def wall(self):
        options = dict(
            connector=TCPConnector(
                keepalive_timeout=299,
                enable_cleanup_closed=True,
            ),
            timeout=ClientTimeout(total=self.settings['cooldown']),
        )

        async with ClientSession(**options) as session:
            try:
                resp = await session.get(
                    self.settings['url'],
                    headers=self.settings['headers'],
                    allow_redirects=False,
                )
            except HTTP_EXCEPTIONS as e:
                if isinstance(e, OSError) and e.errno != ECONNRESET:
                    err_msg = f'Connection error: {str(e)}'
                    await aprint(err_msg, use_stderr=True, flush=True)
                return

            async with resp:
                if resp.status != 200:
                    return

                try:
                    html = await resp.text()
                except TimeoutError:
                    return

                async for post_info in self.extractor(html):
                    yield post_info

    async def fetch(self):
        while True:
            async for pid, urls, name in self.wall():
                if (pid, urls) in self.seen_posts:
                    continue

                for url in urls:
                    if url and name:
                        await aprint(f'{name}\n{url}', flush=True)
                        show = Show(name=name, pid=pid, url=url, title='')
                        if show in self.wanted_shows:
                            self.workers.submit(self.download, show)
                    else:
                        msg = f'Invalid {pid!r} | {name!r} | {url!r}'
                        await aprint(msg, use_stderr=True, flush=True)

            await asyncio.sleep(self.settings['cooldown'])

    async def run(self):
        _wakeup()

        with self.workers:
            try:
                await self.fetch()
            except asyncio.exceptions.CancelledError:
                self.save_seen_posts()
                if len(self.workers._threads):
                    msg = 'Waiting for downloads to finish ...'
                    print(msg, file=sys.stderr, flush=True)


def _wakeup(interval=0.3):
    # Fix for better handling of SIGINT (Ctrl-C) on Windows
    asyncio.get_event_loop().call_later(interval, _wakeup)


def main():
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(Downloader().run())
    except KeyboardInterrupt:
        raise SystemExit(130)


if __name__ == '__main__':
    exit(main())
