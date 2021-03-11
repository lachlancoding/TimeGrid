import unittest
import pendulum as pen
from grid import TimeGrid

class TestGrid(unittest.TestCase):

  def test_to_dt(self):
    tz = "Canada/Eastern"
    g = TimeGrid(9, 29, tz, 10)
    self.assertEqual(pen.from_format("2019-02-01", "YYYY-MM-DD", tz), g.to_dt("2019-02-01"))
    self.assertEqual(pen.from_format("2019-02-01 01:23:45", "YYYY-MM-DD HH:mm:ss", tz), g.to_dt("2019-02-01 01:23:45"))

  def test_align(self):
    g = TimeGrid(9, 29, "Canada/Eastern", 10, align="minute")
    self.assertEqual([(10, 20)], list(g.stamps()))

  def test_iter(self):
    g = TimeGrid(10, 30, "Canada/Eastern", 10)
    self.assertEqual([(10, 20), (20, 30)], list(g))

  def test_index(self):
    g = TimeGrid(10, 30, "Canada/Eastern", 10)
    with self.assertRaises(ValueError):
      g.index(9)
    self.assertEqual(0, g.index(10))
    self.assertEqual(0, g.index(11))
    self.assertEqual(0, g.index(19))
    self.assertEqual(1, g.index(20))
    with self.assertRaises(ValueError):
      g.index(30)

  def test_iter(self):
    g = TimeGrid(9, 78, "Canada/Eastern", "minutes")
    for span in g:
      self.assertEqual(60, (span[1] - span[0]).in_seconds())

  def test_stamps(self):
    g = TimeGrid(9, 78, "Canada/Eastern", "minutes")
    for span in g.stamps():
      self.assertEqual(60, (span[1] - span[0]))

  def test_puff(self):
    for alignment in ["minute", "hour"]:
      g = TimeGrid(12, 19, "Canada/Eastern", ("seconds", 10))
      puffed = g.puff_left()
      self.assertEqual(puffed.start_ts, 12)
      g = TimeGrid(12, 31, "Canada/Eastern", ("seconds", 10), align=alignment)
      puffed = g.puff_left()
      self.assertEqual(puffed.start_ts, 10)
      g = TimeGrid(12, 31, "Canada/Eastern", ("seconds", 10), align=alignment)
      puffed = g.puff_right()
      self.assertEqual(puffed.end_ts, 40)
      g = TimeGrid(12, 31, "Canada/Eastern", ("seconds", 10), align=alignment)
      puffed = g.puff()
      self.assertEqual(puffed.start_ts, 10)
      self.assertEqual(puffed.end_ts, 40)




