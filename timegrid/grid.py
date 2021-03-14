import pendulum as pen
import datetime
import types

class TimeGrid (object):
  
  def to_dt(self, x, tz=None):
    if tz is None:
      tz = self.tz

    if isinstance(x, str):
      try:
        return pen.from_format(x, "YYYY-MM-DD", tz)
      except ValueError:
        pass
      try:
        return pen.from_format(x, "YYYY-MM-DD HH:mm:ss", tz)
      except ValueError:
        pass
      raise ValueError(f"Cannot convert str '{x}' to date")
    elif isinstance(x, int):
      return pen.from_timestamp(x, tz)
    elif isinstance(x, datetime.datetime):
      return x

  def __init__(self, start, end, tz, step, stride=1, align=True):
    self._start = self.to_dt(start, tz=tz)
    self._end = self.to_dt(end, tz=tz)
    self.tz = tz
    self.stride = stride

    if isinstance(step, int):
      self.step = ("seconds", step)
    elif isinstance(step, str):
      self.step = (step, 1)
    else: 
      self.step = step

    if align is True:
      self.align = self.step[0][:-1]
    elif align is False:
      self.align = None
    else:
      self.align = align
    
    if self._start != self._start.start_of(self.align):
      self.start = self._align(self._start)
      if self._start != self.start:
        self.start = self._next_step(self.start)
    else:
      self.start = self._start

    if self._end != self._end.start_of(self.align):
      self.end = self._align(self._end)
    else:
      self.end = self._end
    back_one = self.end.subtract(seconds=1)
    
    try:
      cap = self.at(back_one)
      if cap[1] != self.end:
        self.end = cap[0]
    except ValueError:
      self.end = self.start

  @property
  def start_ts(self):
    return self.start.int_timestamp

  @property
  def end_ts(self):
    return self.end.int_timestamp

  def _align(self, dt):
    align_start = dt.start_of(self.align)
    steps = self._step_distance(align_start, dt)
    return self._jump_from(align_start, steps)

  def _next_step(self, dt):
    return dt.add(**{self.step[0]:self.step[1]})

  def _prev_step(self, dt):
    return dt.subtract(**{self.step[0]:self.step[1]})

  def _jump_from(self, dt, steps):
    return dt.add(**{self.step[0]:steps * self.step[1]})

  def _step_distance(self, dt_from, dt_to):
    return getattr((dt_to - dt_from), self.step[0])//self.step[1]

  def stamps(self): 
    for start_dt, end_dt in self:
      yield (start_dt.int_timestamp, end_dt.int_timestamp)

  def __iter__(self):
    dt = self.start
    while True:
      dt_next = self._next_step(dt)
      if dt_next <= self.end:
        yield (dt, dt_next)
        dt = dt_next
      else:
        break

  def __len__(self):
    if self.end <= self.start:
      return 0
    else:
      return 1 + self.index(self.end.subtract(seconds=1)) - self.index(self.start)

  ''' Return index of bucket containing timestamp '''
  def index(self, target):
    if isinstance(target, int):
      dt = pen.from_timestamp(target, self.tz)
    elif isinstance(target, datetime.datetime):
      dt = target
    else:
      raise TypeError(f"Index target must be int timestamp or datetime, not {type(target)}")

    if self.start <= dt < self.end:
      return getattr((dt - self.start), self.step[0])//self.step[1]
    else:
      raise ValueError(target)

  ''' Return bucket bounds at timestamp '''
  def at(self, target):
    index = self.index(target)
    at_start = self._jump_from(self.start, index)
    at_end = self._next_step(at_start)
    return (at_start, at_end)

  def puff_left(self):
    return TimeGrid(self._prev_step(self.start) if self._start != self.start else self._start, 
      self._end, self.tz, self.step, self.stride, self.align)

  def puff_right(self):
    return TimeGrid(self._start, self._next_step(self.end) if self._end != self.end else self._end, 
      self.tz, self.step, self.stride, self.align)

  def puff(self):
    return TimeGrid(self._prev_step(self.start) if self._start != self.start else self._start,
      self._next_step(self.end) if self._end != self.end else self._end,
      self.tz, self.step, self.stride, self.align)

  ### Data Functions ###
  def bucket_stream(self, data_stream, as_ts=True):
    if not isinstance(data_stream, types.GeneratorType):
      ds_iter = iter(data_stream)
    else:
      ds_iter = data_stream
    bucket = []
    peek_dp = None
    def peek():
      nonlocal peek_dp
      if peek_dp is None:
        peek_dp = next(ds_iter, None)
      return peek_dp

    def take():
      nonlocal peek_dp
      take_dp = peek_dp
      peek_dp = None
      return take_dp

    for bucket_span in self:
      # pull from stream until dp inside bucket
      while peek() is not None and peek()[0] < bucket_span[0].int_timestamp:
        take()        
      # pull from stream until outside bucket
      while peek() is not None and bucket_span[0].int_timestamp <= peek()[0] < bucket_span[1].int_timestamp:
        bucket.append(take())
      yield (bucket_span[0].int_timestamp if as_ts else bucket_span[0], tuple(bucket))
      bucket = []

  def agg_buckets(self, buckets, f):
    for stamp, dps in buckets:
      yield(stamp, f(dps))

  def agg_stream(self, data_stream, f, as_ts=True):
    return self.agg_buckets(self.bucket_stream(data_stream, as_ts), f)



