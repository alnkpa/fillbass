#!/usr/bin/env python

import logging
import os.path

import bs4

LOG = logging.getLogger(__name__)

MLB_URL = "http://gd2.mlb.com/"
DATA_URL = "http://gd2.mlb.com/components/game/mlb/"


def __copy_to_file__(src_url, file_name, session):
    with open(file_name, "wb") as f:
        request = session.get(src_url)
        request.raise_for_status()
        f.write(request.content)


def fetch_game(url, path, session, players_fetched):
    LOG.debug("Fetching [%s] …", url)
    try:
        __copy_to_file__(os.path.join(url, "inning", "inning_all.xml"),
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
                if player_id in players_fetched:
                    continue

                players_fetched.append(player_id)
                __copy_to_file__(os.path.join(player_type_url, link.get("href").strip()),
                                 os.path.join(
                                     path, link.get("href").strip().split("/")[-1]),
                                 session)
    except Exception as e:
        LOG.warning("Encountered {}".format(e))


def fetch_day(save_path, day, session, players_fetched):
    LOG.info("Retrieving [%s] …", day)
    full_url = os.path.join(DATA_URL,
                            "year_%d" % day.year,
                            "month_%02d" % day.month,
                            "day_%02d" % day.day)
    local_dir = os.path.join(save_path,
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
        fetch_game(os.path.join(full_url, game_id),
                   game_path,
                   session,
                   players_fetched)

    LOG.info("Retrieved [%s]", day)


if __name__ == '__main__':
    fetch()
