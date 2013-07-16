import os
import sys

from setuptools import find_packages, setup


here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.rst')).read()
NEWS = open(os.path.join(here, 'NEWS.txt')).read()
VERSION = '0.0.1'


setup(
    name='hive_utils',
    version=VERSION,
    classifiers=['License :: OSI Approved :: BSD License'],
    long_description=README + '\n\n' + NEWS,
    url='https://github.com/eventbrite/python_hive_utils',
    license='BSD',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    include_package_data=True,
    zip_safe=True,
    entry_points={},
    install_requires = ['hive-thrift-py'],
    extras_require = {
        'hive-thrift-py':  ['hive-thrift-py==0.0.1'],
    },
)
