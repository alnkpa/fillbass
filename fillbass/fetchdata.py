import datetime
import os.path
import bs4
import argparse
import requests
from shutil import copyfileobj
import logging

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.FileHandler("fetch.log")
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter(
    '%(asctime)s %(name)s [%(levelname)s] %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
LOG.addHandler(ch)

MLB_URL = "http://gd2.mlb.com/"
DATA_URL = "http://gd2.mlb.com/components/game/mlb/"


class Fetcher(object):

    def __init__(self):
        super(Fetcher, self).__init__()
        self.players_fetched = []
        self.save_path = ""

    def __copy_to_file__(src_url, file_name, session):
        with open(file_name, "wb") as f:
            request = session.get(src_url, stream=True)
            request.raise_for_status()
            copyfileobj(request.raw, f)

    def fetch_game(self, url, path, session):
        try:
            Fetcher.__copy_to_file__(os.path.join(url, "inning", "inning_all.xml"),
                                     os.path.join(path, "inning_all.xml"),
                                     session)

            for player_type in ["pitchers", "batters"]:
                player_type_url = os.path.join(url, player_type)
                request = session.get(player_type_url)
                request.raise_for_status()
                soup = bs4.BeautifulSoup(request.text, "lxml")
                for link in soup.find_all("a"):
                    if link.string.strip().startswith("P"):
                        continue

                    player_id = int(
                        link.get("href").strip().split("/")[-1].split(".")[0])
                    if player_id in self.players_fetched:
                        continue

                    self.players_fetched.append(player_id)
                    Fetcher.__copy_to_file__(os.path.join(player_type_url, link.get("href").strip()),
                                             os.path.join(
                                                 path, link.get("href").strip().split("/")[-1]),
                                             session)
        except Exception as e:
            LOG.warning("Encountered {}".format(e))

    def fetch_day(self, day, session):
        full_url = os.path.join(DATA_URL,
                                "year_%d" % day.year,
                                "month_%02d" % day.month,
                                "day_%02d" % day.day)
        local_dir = os.path.join(self.save_path,
                                 "year_%d" % day.year,
                                 "month_%02d" % day.month,
                                 "day_%02d" % day.day)

        if os.path.isdir(local_dir):
            return

        os.makedirs(local_dir)

        day_http = session.get(full_url)
        soup = bs4.BeautifulSoup(day_http.text, "lxml")

        for link in soup.find_all("a"):
            game_id = link.string.strip()
            if not game_id.startswith("gid"):
                continue

            game_path = os.path.join(local_dir, game_id)
            os.makedirs(game_path)
            self.fetch_game(os.path.join(full_url, game_id),
                            game_path,
                            session)

        LOG.info("Retrieved {}".format(day))

    def run(self):
        ONE_DAY = datetime.timedelta(days=1)

        with requests.Session() as session:
            while self.start_date <= self.end_date:
                try:
                    self.fetch_day(self.start_date, session)
                except Exception as e:
                    LOG.error(
                        "Encountered {} while fetching {}".format(e, self.start_date))
                self.start_date += ONE_DAY

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--start-date", help="""fetch data beginning from this day. Format as 'DD/MM/YYYY'.
                                                Defaults to 01/01/2008""",
                        default="01/01/2008")
    parser.add_argument("-e", "--end-date", help="""fetch data up to and including this day. Format as 'DD/MM/YYYY'.
                                              Defaults to 01/01/2009""",
                        default="01/01/2009")
    parser.add_argument("save_dir", help="""This program saves the xml files in this directory.
                                            Defaults to 'data'""",
                        nargs="?",
                        default="data")
    args = parser.parse_args()
    fetcher = Fetcher()
    fetcher.save_path = args.save_dir
    start_date = datetime.datetime.strptime(args.start_date, "%d/%m/%Y").date()
    end_date = datetime.datetime.strptime(args.end_date, "%d/%m/%Y").date()
    fetcher.start_date = start_date
    fetcher.end_date = end_date
    fetcher.run()
