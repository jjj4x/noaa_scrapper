from argparse import ArgumentParser
from dataclasses import asdict, dataclass, field
from io import BytesIO
from gzip import open as gzip_open
from logging import config as logging_config, getLogger
from multiprocessing import Process, Queue
from os import path as os_path, remove
from pathlib import Path
from queue import Empty, Full
from re import findall, match
from typing import Tuple
from tarfile import open as tar_open
from typing import Dict, List, Optional
from time import monotonic, sleep

from requests import get

SELF_PATH = Path(__file__)
DEFAULT_YEARS = ['1901', '1902']
LOG = getLogger(__name__)
LOG_CONF = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'root': {
            'format': (
                'SCRAP_NOAA[%(process)d] '
                + '[%(levelname)s] '
                + '[%(name)s] '
                + '%(message)s '
            ),
            'datefmt': '%Y-%m-%dT%H:%M:%S',
            'class': 'logging.Formatter',
        },
    },
    'handlers': {
        'stream': {
            'class': 'logging.StreamHandler',
            'formatter': 'root',
            'stream': 'ext://sys.stderr',
        },
    },
    'root': {
        'handlers': ['stream'],
        'level': 'INFO',
    },
}


@dataclass
class Config:
    # *********************************Defaults*********************************
    DEFAULT_URL = 'https://www.ncei.noaa.gov/data/global-hourly/archive/isd/'
    DEFAULT_INDEX_REGEX = r'>(isd_\d{4}_c.*.tar.gz)<'
    DEFAULT_MEMBER_REGEX = r'\d+-\d+-\d+'
    DEFAULT_RUN_TIME_MAX = 300  # 5 minutes
    DEFAULT_WORKERS_COUNT = 2
    DEFAULT_POLLING_TIMEOUT = 2
    DEFAULT_TERMINATE_TIMEOUT = 2.
    DEFAULT_TMP_DIR = Path('/tmp/noaa_stuff')
    # **************************************************************************

    # *********************************Options*********************************
    logging: Dict = field(default_factory=lambda: LOG_CONF)
    url: str = field(default=DEFAULT_URL)
    index_regex: str = field(default=DEFAULT_INDEX_REGEX)
    member_regex: str = field(default=DEFAULT_MEMBER_REGEX)
    run_time_max: int = field(default=DEFAULT_RUN_TIME_MAX)
    workers_count: int = field(default=DEFAULT_WORKERS_COUNT)
    polling_timeout: float = field(default=DEFAULT_POLLING_TIMEOUT)
    terminate_timeout: float = field(default=DEFAULT_TERMINATE_TIMEOUT)
    years: List[set] = field(default_factory=lambda: DEFAULT_YEARS)
    force: bool = field(default=False)
    tmp_dir: Path = field(default=DEFAULT_TMP_DIR)
    is_compress: bool = field(default=False)
    # *************************************************************************

    def as_dict(self):
        return asdict(self)

    @classmethod
    def normalize_years(cls, inp: str):
        years = inp.split(',') if ',' in inp else inp.split('-')
        if '-' in inp:
            years = range(int(years[0]), int(years[1]) + 1)
        return sorted(str(y) for y in years)

    @classmethod
    def normalize_url(cls, inp: str):
        return inp.rstrip('/') + '/'

    @classmethod
    def configure(cls, cli=None) -> 'Config':
        args = (cli or CLI).parse_args().__dict__ or {}
        return cls.activate_logging(Config(**args))

    @classmethod
    def activate_logging(cls, config: 'Config') -> 'Config':
        """
        Activate global logging settings.
        :param config: the conf
        :return: the same conf
        """
        log = config.logging

        logging_config.dictConfig(log)

        root_level = log['root']['level']
        root_handlers = ', '.join(log['root']['handlers'])

        LOG.info(f'The Root Logger [{root_level}] handlers: {root_handlers}')

        return config


CLI = ArgumentParser()
CLI.add_argument(
    '--url',
    dest='url',
    default=Config.DEFAULT_URL,
    type=Config.normalize_url,
)
CLI.add_argument(
    '--index-regex',
    dest='index_regex',
    default=Config.DEFAULT_INDEX_REGEX,
)
CLI.add_argument(
    '--member-regex',
    dest='member_regex',
    default=Config.DEFAULT_MEMBER_REGEX,
)
CLI.add_argument(
    '--run-time-max',
    dest='run_time_max',
    default=Config.DEFAULT_RUN_TIME_MAX,
    type=int,
)
CLI.add_argument(
    '--workers-count',
    dest='workers_count',
    default=Config.DEFAULT_WORKERS_COUNT,
    type=int,
)
CLI.add_argument(
    '--polling-timeout',
    dest='polling_timeout',
    default=Config.DEFAULT_POLLING_TIMEOUT,
    type=float,
)
CLI.add_argument(
    '--terminate_timeout',
    dest='terminate_timeout',
    default=Config.DEFAULT_TERMINATE_TIMEOUT,
    type=float,
)
CLI.add_argument(
    '--years',
    dest='years',
    default=DEFAULT_YEARS,
    type=Config.normalize_years,
    help='For example: --years 1901; --years 1901,1902; --years 1901-1930',
)
CLI.add_argument(
    '--force',
    action='store_true',
    dest='force',
    default=False,
    help='Force overwrite files if they already exist.',
)
# noinspection PyTypeChecker
CLI.add_argument(
    '--tmp-dir',
    dest='tmp_dir',
    default=Config.DEFAULT_TMP_DIR.absolute(),
    type=Path,
    help='Directory for dumping temporary data (tarball extraction).',
)
CLI.add_argument(
    '--is-compress',
    action='store_true',
    dest='is_compress',
    default=False,
    help='If set, the result will be gzipped in filename like "1901.gz". Else '
         'it will be saved as plaintext into "1901".',
)


class Worker(Process):
    def __init__(
        self,
        worker_number: int,
        conf: Config,
        queue: 'Queue[Tuple[str, str]]',  # (year, filename)
        queue_done: 'Queue[str]',
        daemon=True,
        **kwargs,
    ):
        super().__init__(daemon=daemon, **kwargs)

        self.worker_number = worker_number
        self.conf = conf
        self.queue = queue
        self.queue_done = queue_done

    def run(self):
        """Worker loop."""
        while True:
            try:
                year, filename = self.queue.get(timeout=2, block=True)
            except Empty:
                continue

            LOG.info('Fetching %s', self.conf.url + filename)

            res = get(self.conf.url + filename, stream=True)
            if not (200 <= res.status_code < 300):
                LOG.warning('Cannot download %s for some reason', filename)
                self.queue_done.put(year, block=False)
                continue

            result = f'./{year}.gz' if self.conf.is_compress else f'./{year}'

            if os_path.isfile(result) and not self.conf.force:
                LOG.info('The ./%s.txt is already exists', year)
                self.queue_done.put(year, block=False)
                continue

            if os_path.isfile(result) and self.conf.force:
                LOG.info('The ./%s.txt is already exists; removing', year)
                remove(result)

            tmp = self.conf.tmp_dir / year
            tmp.mkdir(exist_ok=True, parents=True)

            LOG.info('Dumping %s into %s', filename, tmp.absolute())

            with tar_open(fileobj=BytesIO(res.raw.read()), mode='r:gz') as tar:
                data = (m.name for m in tar.getmembers())
                data = (m for m in data if match(self.conf.member_regex, m))
                tar.extractall(tmp.absolute())

            _open = gzip_open if self.conf.is_compress else open
            with _open(result, 'ab') as fd_result:
                for data_file in data:
                    LOG.info('Aggregating %s into %s', data_file, result)
                    with (tmp / data_file).open(mode='rb') as fd_data_file:
                        fd_result.write(fd_data_file.read())

            self.queue_done.put(year, block=False)


class Master:
    """Master process and load balancer."""

    def __init__(self, conf: Config):
        workers = [None for _ in range(conf.workers_count)]
        self.workers: List[Optional[Worker]] = workers
        self.queue: 'Queue[Tuple[str, str]]' = Queue()
        self.queue_done: 'Queue[str]' = Queue()
        self.conf = conf

    def start(self):
        """Master loop."""
        res = get(self.conf.url)
        if not (200 <= res.status_code < 300):
            res.raise_for_status()

        index = {}
        for filename in findall(self.conf.index_regex, res.text):
            # filename == 'isd_1901_c20180826T025524.tar.gz'
            _, year, *_ = filename.split('_')
            index[year] = filename

        for number in range(self.conf.workers_count):
            worker = Worker(number, self.conf, self.queue, self.queue_done)
            worker.start()
            self.workers[number] = worker

        pending = {year for year in self.conf.years if year in index}
        if not pending:
            LOG.warning(
                'Cannot fetch %s. The available years are: %s.',
                self.conf.years,
                index.values(),
            )
            raise RuntimeError('There is nothing to do.')

        for year in pending:
            try:
                self.queue.put((year, index[year]), block=False)
            except Full:
                LOG.warning('Cannot add %s; the queue is full.', year)
                continue

        start = monotonic()
        while pending:
            if monotonic() - start > self.conf.run_time_max:
                raise RuntimeError('There is some dangling work.')

            if any(not w.is_alive() for w in self.workers):
                raise RuntimeError('Some worker is dead :(.')

            while not self.queue_done.empty():
                pending.discard(self.queue_done.get(block=False))

            LOG.info(f'Waiting for {pending}.',)
            sleep(self.conf.polling_timeout)

        LOG.info('All done')

    def stop(self):
        LOG.info('Shutting down master process.')

        self.queue.close()
        self.queue_done.close()

        workers = [w for w in self.workers if w is not None]

        LOG.info('Trying to terminate workers.')
        for worker in workers:
            if worker.is_alive():
                worker.terminate()

        sleep(self.conf.terminate_timeout)

        if not any(p.is_alive() for p in workers):
            return

        LOG.warning('Some workers did not stop. Killing all the workers :(.')
        for worker in workers:
            if worker.is_alive():
                worker.kill()


def main(conf=None):
    master = Master(conf or Config.configure())
    try:
        master.start()
    finally:
        master.stop()


if __name__ == '__main__':
    main()
