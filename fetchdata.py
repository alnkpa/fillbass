import datetime
import os.path
import urllib.request
import bs4
import argparse

MLB_URL = "http://gd2.mlb.com/"
DATA_URL = "http://gd2.mlb.com/components/game/mlb/"
LOCAL_ROOT = ""
PLAYERS_FETCHED = []

def fetch_game(url, path):
	try:
		urllib.request.urlretrieve(os.path.join(url, "inning", "inning_all.xml"),
		                           filename=os.path.join(path, "inning_all.xml"))

		pitcher_url = os.path.join(url, "pitchers")
		soup = bs4.BeautifulSoup(urllib.request.urlopen(pitcher_url).read(), "lxml")
		for link in soup.find_all("a"):
			if link.string.strip().startswith("P"):
				continue

			pitcher_id = int(link.get("href").strip().split("/")[-1].split(".")[0])
			if pitcher_id in PLAYERS_FETCHED:
				continue

			PLAYERS_FETCHED.append(pitcher_id)
			urllib.request.urlretrieve(os.path.join(pitcher_url, link.get("href").strip()),
			                           filename=os.path.join(path, link.get("href").strip().split("/")[-1]))
	except Exception as e:
		print(e)

def fetch_day(day):
	full_url = os.path.join(DATA_URL,
	                        "year_%d" % day.year,
	                        "month_%02d" % day.month,
	                        "day_%02d" % day.day)
	local_dir = os.path.join(LOCAL_ROOT,
	                         "year_%d" % day.year,
	                        "month_%02d" % day.month,
	                        "day_%02d" % day.day)
	print(local_dir)
	if os.path.isdir(local_dir):
		return

	os.makedirs(local_dir)

	day_http = urllib.request.urlopen(full_url)
	day_html = day_http.read()
	soup = bs4.BeautifulSoup(day_html, "lxml")

	for link in soup.find_all("a"):
		game_id = link.string.strip()
		if not game_id.startswith("gid"):
			continue

		game_path = os.path.join(local_dir, game_id)
		os.makedirs(game_path)
		fetch_game(os.path.join(full_url, game_id),
		           game_path)

def fetch_data(start_date, end_date):
	ONE_DAY = datetime.timedelta(days=1)

	while start_date <= end_date:
		try:
			fetch_day(start_date)
		except Exception as e:
			print(e)
		start_date += ONE_DAY

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
	LOCAL_ROOT = args.save_dir
	start_date = datetime.datetime.strptime(args.start_date, "%d/%m/%Y").date()
	end_date = datetime.datetime.strptime(args.end_date, "%d/%m/%Y").date()
	fetch_data(start_date, end_date)
