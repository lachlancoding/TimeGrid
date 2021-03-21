from setuptools import setup

setup(
  name="TimeGrid",
  version="0.0.1",
  author="Lachlan Standing",
  author_email="lachlanstanding@gmail.com",
  description="Like a range, but for dates!",
  url="https://github.com/lachlancoding/TimeGrid",
  packages=['timegrid'],
  install_requires=[
    'pendulum'
  ])