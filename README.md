# fillbass

Small python files to print pitches.

## Dependencies

- [bs4](http://www.crummy.com/software/BeautifulSoup/#Download)
- [matplotlib](http://matplotlib.org/downloads.html)
- [sqlalchemy](http://www.sqlalchemy.org/download.html)

## Usage

```
[patrick@arch fillbass]$ python fetchdata.py # -h # This takes a while
…
[patrick@arch fillbass]$ python parsedata.py -d fillbass.db scan data
…
[patrick@arch fillbass]$ python parsedata.py -d fillbass.db list C
ID	Name
110450	Cory Bailey
112020	Chris Carpenter
…
[patrick@arch fillbass]$ python parsedata.py -d fillbass.db list C K
ID	Name
446372	Corey Kluber
461317	Chris Kinsey
477132	Clayton Kershaw
…
[patrick@arch fillbass]$ python parsedata.py -d fillbass.db show 477132
evaluated 6007 pitches
```
