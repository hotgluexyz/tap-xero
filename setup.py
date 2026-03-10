#!/usr/bin/env python
from setuptools import setup

setup(name="tap-xero",
      version="2.2.12",
      description="Singer.io tap for extracting data from the Xero API",
      author="Stitch",
      url="http://singer.io",
      classifiers=["Programming Language :: Python :: 3 :: Only"],
      py_modules=["tap_xero"],
      install_requires=[
          "hotglue-singer-sdk>=1.0.13,<2.0.0",
          "requests==2.29.0",
      ],
      extras_require={
          'dev': [
              'ipdb',
              'pylint',
              'nose'
          ]
      },
      entry_points="""
          [console_scripts]
          tap-xero=tap_xero:main
      """,
      packages=["tap_xero"],
      package_data = {
          "schemas": ["tap_xero/schemas/*.json"]
      },
      include_package_data=True,
)
