from setuptools import setup, find_packages

with open('README') as f:
	long_description = f.read()

setup(
	name='dbq',
	version='0.1',
	description='DB generic query/scan framework for AQL and ES databases',
	long_description=long_description,
	licence='Bharti Airtel',
	author='Pankaj Saini',
	author_email='pankaj.saini@airtel.com',
	packages=find_packages(),
	python_requires=">=2.7",
	install_requires=[
		"aerospike",
		"elasticsearch-dsl",
		"python-dateutil"
	]
)