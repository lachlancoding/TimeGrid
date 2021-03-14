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

  def test_iteration(self):
    tses = lambda x: ((a.int_timestamp, b.int_timestamp) for a,b in x)
    g = TimeGrid(10, 30, "Canada/Eastern", 10)
    self.assertEqual([(10, 20), (20, 30)], list(tses(g)))
    g = TimeGrid(10, 29, "Canada/Eastern", 10)
    self.assertEqual([(10, 20)], list(tses(g)))
    g = TimeGrid(11, 29, "Canada/Eastern", 10)
    self.assertEqual([(11, 21)], list(tses(g)))
    g = TimeGrid(11, 29, "Canada/Eastern", 10, align="minute")
    self.assertEqual([], list(tses(g)))

  def test_len(self):
    self.assertEqual(len(TimeGrid(10, 30, "Canada/Eastern", 10)), 2)
    g = TimeGrid(10, 29, "Canada/Eastern", 10)
    self.assertEqual(len(g), 1)
    g = TimeGrid(10, 19, "Canada/Eastern", 10)
    self.assertEqual(len(g), 0)

  def test_index(self):
    g = TimeGrid(10, 30, "Canada/Eastern", 10)
    with self.assertRaises(ValueError):
      g.index(9)
    self.assertEqual(0, g.index(10))
    self.assertEqual(0, g.index(11))
    self.assertEqual(0, g.index(19))
    self.assertEqual(1, g.index(20))
    self.assertEqual(1, g.index(29))
    with self.assertRaises(ValueError):
      g.index(30)
    with self.assertRaises(TypeError):
      g.index("Wednesday the 1st of april 2018 in england")

    g = TimeGrid(9, 31, "Canada/Eastern", 10, align="minute")
    with self.assertRaises(ValueError):
      g.index(9)
    self.assertEqual(0, g.index(10))
    self.assertEqual(0, g.index(11))
    self.assertEqual(0, g.index(19))
    self.assertEqual(1, g.index(20))
    self.assertEqual(1, g.index(29))
    with self.assertRaises(ValueError):
      g.index(30)

  def test_at(self):
    g = TimeGrid(10, 30, "Canada/Eastern", 10)
    with self.assertRaises(ValueError):
      g.at(9)
    with self.assertRaises(ValueError):
      g.at(30)
    self.assertEqual(g.start, g.at(10)[0])
    self.assertEqual(g.at(10)[1], g.at(20)[0])
    self.assertEqual(g.start.add(seconds=10), g.at(20)[0])

    g = TimeGrid(9, 31, "Canada/Eastern", 10, align="minute")
    with self.assertRaises(ValueError):
      g.at(9)
    with self.assertRaises(ValueError):
      g.at(30)
    self.assertEqual(g.start, g.at(10)[0])
    self.assertEqual(g.at(10)[1], g.at(20)[0])
    self.assertEqual(g.start.add(seconds=10), g.at(20)[0])
    with self.assertRaises(TypeError):
      g.index("Wednesday the 1st of april 2018 in england")

  def test_iter(self):
    g = TimeGrid(9, 78, "Canada/Eastern", "minutes")
    for span in g:
      self.assertEqual(60, (span[1] - span[0]).in_seconds())

  def test_stamps(self):
    # TODO: This is auto aligned to the minute and therefore ecompasses 0 complete minutes
    # so index fails and so does construction trying to align the end to a complete minute
    # I actually intended it to not align but also it should handle this gracefully.
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

  def test_bucket_data(self):
    g = TimeGrid(10, 30, "Canada/Eastern", ("seconds", 10))
    buckets = list(g.bucket_stream([(9, 3), (10, 1), (15, 2), (20, 3), (23, 2), (30, 1)]))
    self.assertEqual([
      (10, ((10, 1), (15, 2))), 
      (20, ((20, 3), (23, 2)))], buckets)

  def test_agg_buckets(self):
    g = TimeGrid(10, 30, "Canada/Eastern", ("seconds", 10))
    agg_buckets = list(g.agg_stream([(9, 3), (10, 1), (15, 2), (20, 3), (23, 2), (30, 1)], lambda dps: sum([x[1] for x in dps])))
    self.assertEqual([(10, 3), (20, 5)], agg_buckets)
      