import sys
import platform

import PySide
from PySide.QtGui import QApplication, QMainWindow, QProgressBar

from ui_fillbass import Ui_MainWindow
import fetchdata
import parsedata
import datetime

class MainWindow(QMainWindow, Ui_MainWindow):
	def __init__(self, parent=None):
		super(MainWindow, self).__init__(parent)
		self.setupUi(self)
		self.actionAbout_Qt.triggered.connect(QApplication.aboutQt)
		self.pushButton.clicked.connect(self.fetch_files)

	@PySide.QtCore.Slot()
	def fetch_files(self):
		start_date = self.startDateEdit.date().toPython()
		end_date = self.endDateEdit.date().toPython()
		self.fetcher = fetchdata.Fetcher()
		self.fetcher.save_path = PySide.QtGui.QFileDialog.getExistingDirectory(self)
		self.fetcher.start_date = start_date
		self.fetcher.end_date = end_date
		self.fetcher.fetching_day.connect(self.fetching_day)
		self.progress = QProgressBar()
		self.progress.setRange((start_date - end_date).days, 0)
		self.progress.reset()
		self.statusbar.addWidget(self.progress, stretch=0.2)
		self.fetcher.start()

	@PySide.QtCore.Slot(int)
	def fetching_day(self, days_left):
		self.progress.setValue(days_left)

if __name__ == "__main__":
	app = QApplication(sys.argv)
	frame = MainWindow()
	frame.show()
	app.exec_()
