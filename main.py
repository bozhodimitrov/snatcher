import asyncio
from concurrent.futures import ThreadPoolExecutor
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
from lxml.etree import ParserError
from lxml.etree import strip_elements
from lxml.etree import XMLSyntaxError
from lxml.html import fromstring
from lxml.html import HTMLParser


asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

CONFIG_FILE = 'config.toml'

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
            key, data = value, None

        found = super().__contains__(key)
        self[key] = data
        return found


class Wanted(set):
    def __init__(self, items):
        self.items = {item.lower(): item for item in items}

    def __contains__(self, value):
        for item in self.items:
            if item in value.name.lower():
                value.title = self.items[item]
                return True
        else:
            return False


class Downloader:
    def __init__(self):
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
            return toml.load(CONFIG_FILE)
        except (OSError, toml.TomlDecodeError):
            raise SystemExit('Invalid config')

    def save_seen_posts(self):
        with open(CONFIG_FILE, 'w') as f:
            self.config['last'].update(self.seen_posts)
            toml.dump(self.config, f)

    def dl_link(self, links):
        if isinstance(links, str):
            links = (links,)

        for src in self.settings['source_priority']:
            for link in links:
                if src in link:
                    return str(link)

    def download(self, show):
        dl_path = f'{self.settings["download_dir"]}/{show.title}'
        opts = {
            **self.default_ytdl_opts,
            'outtmpl': f'{dl_path}/{youtube_dl.utils.DEFAULT_OUTTMPL}',
        }

        with youtube_dl.YoutubeDL(opts) as yt:
            try:
                result = yt.download([show.url])
            except youtube_dl.utils.DownloadError:
                result = 1

        if result:
            print(f'Error while downloading: {show.url}')
        else:
            print(f'Download finish: {show.name}')

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
            link = self.dl_link(links)
            strip_elements(tag, 'a')
            names = (
                el.strip() for el in tag.xpath(self.settings['path']['name'])
            )
            name = ' '.join(el for el in names if el)

            yield post_id, link, name

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
                    await aprint(err_msg, use_stderr=True)
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
            async for pid, url, name in self.wall():
                if (pid, url) in self.seen_posts:
                    continue

                if url and name:
                    await aprint(f'{name}\n{url}')

                    show = Show(name=name, pid=pid, url=url, title='')
                    if show in self.wanted_shows:
                        self.workers.submit(self.download, show)
                else:
                    await aprint(f'Invalid {pid!r} | {name!r} | {url!r}')

            await asyncio.sleep(self.settings['cooldown'])

    async def run(self):
        with self.workers:
            try:
                await self.fetch()
            except asyncio.exceptions.CancelledError:
                self.save_seen_posts()
                if len(self.workers._threads):
                    print('Waiting for download(s) to finish ...')


def main():
    try:
        asyncio.run(Downloader().run())
    except KeyboardInterrupt:
        raise SystemExit(130)


if __name__ == '__main__':
    exit(main())
